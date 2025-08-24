import requests
import pandas as pd
import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SPREADSHEET_ID = '1YVJKTo8PDKLFqp7azkY1XhqizFRxY0GZB4RvSQe7KEA'
SERVICE_ACCOUNT_FILE = 'eth-options-key.json'

def get_sheets_client():
    """Initialize Google Sheets client"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Error initializing sheets client: {e}")
        return None

def fetch_eth_options_data():
    """Fetch ETH options data with enhanced duplicate removal and OI debugging"""
    try:
        url = "https://api.delta.exchange/v2/tickers"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        tickers = data.get('result', [])
        logger.info(f"Total tickers fetched from API: {len(tickers)}")

        eth_options = []
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)

        # Get ETH spot price
        eth_price = 0
        for ticker in tickers:
            if ticker.get('symbol') == 'ETHUSD':
                eth_price = float(ticker.get('mark_price', 0) or 0)
                break
        logger.info(f"ETH price: {eth_price}")

        successful_parses = 0
        failed_parses = 0
        debug_count = 0

        for ticker in tickers:
            symbol = ticker.get('symbol', '')

            if 'ETH' in symbol and (symbol.startswith('C-') or symbol.startswith('P-')):
                try:
                    # Get strike price
                    strike_price = ticker.get('strike_price')
                    if strike_price:
                        strike = float(strike_price)
                    else:
                        parts = symbol.split('-')
                        if len(parts) < 4:
                            failed_parses += 1
                            continue
                        strike = float(parts[2])

                    # Get expiry date
                    expiry_str = ticker.get('expiry_date') or ticker.get('settlement_time')
                    if expiry_str:
                        if 'T' in expiry_str:
                            expiry_date = datetime.datetime.fromisoformat(expiry_str.replace('Z', '+00:00')).date()
                        else:
                            expiry_date = datetime.datetime.strptime(expiry_str, '%Y-%m-%d').date()
                    else:
                        parts = symbol.split('-')
                        if len(parts) < 4:
                            failed_parses += 1
                            continue
                        expiry_str = parts[3]
                        if len(expiry_str) == 6:
                            expiry_date = datetime.datetime.strptime(expiry_str, '%d%m%y').date()
                        else:
                            failed_parses += 1
                            continue

                    option_type = 'Call' if symbol.startswith('C-') else 'Put'
                    close_price = float(ticker.get('mark_price', 0) or 0)

                    # ENHANCED OI DEBUGGING AND MAPPING
                    oi_contracts = ticker.get('oi_contracts')
                    oi = ticker.get('oi')
                    
                    # Debug first 5 entries to see actual API values
                    if debug_count < 5:
                        logger.info(f"DEBUG {symbol}: oi_contracts='{oi_contracts}', oi='{oi}'")
                        debug_count += 1
                    
                    # Try multiple OI field sources
                    open_interest = 0
                    if oi_contracts and str(oi_contracts) not in ['0', '0.0', '']:
                        try:
                            open_interest = int(float(str(oi_contracts)))
                        except (ValueError, TypeError):
                            open_interest = 0
                    elif oi and str(oi) not in ['0', '0.0', '']:
                        try:
                            # Convert scaled OI back to contracts (multiply by 100)
                            open_interest = int(float(str(oi)) * 100)
                        except (ValueError, TypeError):
                            open_interest = 0

                    # Use API timestamp
                    if ticker.get('time'):
                        api_time = datetime.datetime.fromisoformat(ticker['time'].replace('Z', '+00:00'))
                        date_str = (api_time + datetime.timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')
                        time_str = (api_time + datetime.timedelta(hours=5, minutes=30)).strftime('%H:%M:%S')
                    else:
                        date_str = current_time.strftime('%Y-%m-%d')
                        time_str = current_time.strftime('%H:%M:%S')

                    option_data = {
                        'SYMBOL': symbol,
                        'Date': date_str,
                        'Time': time_str,
                        'Future_Price': eth_price,
                        'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
                        'Strike': strike,
                        'Option_Type': option_type,
                        'Close': close_price,
                        'OI': open_interest
                    }

                    eth_options.append(option_data)
                    successful_parses += 1

                    if successful_parses <= 3:
                        logger.info(f"SUCCESS #{successful_parses}: {symbol} -> OI:{open_interest}")

                except Exception as e:
                    failed_parses += 1
                    logger.info(f"Error parsing {symbol}: {e}")
                    continue

        logger.info(f"Successful parses: {successful_parses}, Failed: {failed_parses}")

        df = pd.DataFrame(eth_options)
        logger.info(f"Raw DataFrame: {len(df)} rows")

        # ENHANCED DUPLICATE REMOVAL - Multiple passes
        # Pass 1: Remove exact duplicates
        df_step1 = df.drop_duplicates()
        logger.info(f"After exact duplicate removal: {len(df_step1)} rows")

        # Pass 2: Remove symbol-based duplicates (keep last)
        df_step2 = df_step1.drop_duplicates(subset=['SYMBOL'], keep='last')
        logger.info(f"After symbol duplicate removal: {len(df_step2)} rows")

        # Pass 3: Remove comprehensive duplicates
        df_unique = df_step2.drop_duplicates(
            subset=['SYMBOL', 'Date', 'Time', 'Strike', 'Option_Type'], 
            keep='last'
        )
        logger.info(f"After comprehensive duplicate removal: {len(df_unique)} rows")

        # Sort the data
        df_sorted = df_unique.sort_values(
            by=['Expiry_Date', 'Time', 'SYMBOL'], 
            ascending=[True, True, True]
        )

        logger.info(f"FINAL: {len(df_sorted)} unique records")
        
        # Log OI statistics
        non_zero_oi = df_sorted[df_sorted['OI'] > 0]['OI'].count()
        logger.info(f"Records with non-zero OI: {non_zero_oi} out of {len(df_sorted)}")

        return df_sorted

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def get_previous_data(worksheet):
    """Get previous hour's data from Google Sheets"""
    try:
        all_records = worksheet.get_all_records()
        if not all_records:
            return pd.DataFrame()
        df = pd.DataFrame(all_records)
        return df.tail(300)
    except Exception as e:
        logger.error(f"Error getting previous data: {e}")
        return pd.DataFrame()

def calculate_open_and_oi_change(current_df, previous_df):
    """Calculate Open and OI_Change based on previous data"""
    if previous_df.empty:
        current_df['Open'] = ''
        current_df['OI_Change'] = ''
        return current_df

    previous_df['Close'] = pd.to_numeric(previous_df['Close'], errors='coerce')
    previous_df['OI'] = pd.to_numeric(previous_df['OI'], errors='coerce')

    merged = current_df.merge(
        previous_df[['SYMBOL', 'Close', 'OI']],
        on='SYMBOL',
        how='left',
        suffixes=('', '_prev')
    )

    merged['Open'] = merged['Close_prev'].fillna('')
    merged['OI_Change'] = (merged['OI'] - merged['OI_prev'].fillna(merged['OI'])).fillna('')

    merged.loc[merged['Close_prev'].isna(), 'Open'] = ''
    merged.loc[merged['OI_prev'].isna(), 'OI_Change'] = ''

    columns_to_keep = ['SYMBOL', 'Date', 'Time', 'Future_Price', 'Expiry_Date',
                       'Strike', 'Option_Type', 'Close', 'OI', 'Open', 'OI_Change']
    return merged[columns_to_keep]

def append_to_sheets(df, worksheet):
    """Append data to Google Sheets"""
    try:
        values = df.values.tolist()
        worksheet.append_rows(values, value_input_option='USER_ENTERED')
        logger.info(f"Appended {len(values)} rows to Google Sheets")
        return True
    except Exception as e:
        logger.error(f"Error appending to sheets: {e}")
        return False

def main():
    """Main data collection function"""
    logger.info("🚀 Starting ETH options data collection - ENHANCED DEBUG VERSION")

    client = get_sheets_client()
    if not client:
        logger.error("Failed to initialize Google Sheets client")
        return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.sheet1

        current_df = fetch_eth_options_data()
        if current_df.empty:
            logger.warning("No data collected")
            return

        previous_df = get_previous_data(worksheet)
        final_df = calculate_open_and_oi_change(current_df, previous_df)

        success = append_to_sheets(final_df, worksheet)
        if success:
            logger.info(f"✅ Successfully collected and updated {len(final_df)} rows")
        else:
            logger.error("❌ Failed to update Google Sheets")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
