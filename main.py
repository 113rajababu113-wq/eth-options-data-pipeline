import requests
import pandas as pd
import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
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
    """Fetch ETH options data using API fields directly (avoid symbol parsing)"""
    try:
        # Use basic tickers endpoint
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

        # Process ETH options using API fields directly
        successful_parses = 0
        failed_parses = 0

        for ticker in tickers:
            symbol = ticker.get('symbol', '')

            # Filter for ETH options
            if 'ETH' in symbol and (symbol.startswith('C-') or symbol.startswith('P-')):
                try:
                    # METHOD 1: Use direct API fields (preferred)
                    strike_price = ticker.get('strike_price')
                    if strike_price:
                        strike = float(strike_price)
                    else:
                        # METHOD 2: Parse from symbol as fallback
                        parts = symbol.split('-')
                        if len(parts) < 4:
                            failed_parses += 1
                            logger.info(f"Symbol {symbol} has only {len(parts)} parts")
                            continue
                        strike = float(parts[2])

                    # Get expiry date from API or parse from symbol
                    expiry_str = ticker.get('expiry_date') or ticker.get('settlement_time')
                    if expiry_str:
                        # Parse ISO date from API
                        if 'T' in expiry_str:  # ISO format with time
                            expiry_date = datetime.datetime.fromisoformat(expiry_str.replace('Z', '+00:00')).date()
                        else:  # Simple date format
                            expiry_date = datetime.datetime.strptime(expiry_str, '%Y-%m-%d').date()
                    else:
                        # Fallback: parse from symbol
                        parts = symbol.split('-')
                        if len(parts) < 4:
                            failed_parses += 1
                            continue
                        expiry_str = parts[3]
                        if len(expiry_str) == 6:
                            expiry_date = datetime.datetime.strptime(expiry_str, '%d%m%y').date()
                        else:
                            failed_parses += 1
                            logger.info(f"Invalid expiry format {expiry_str} in {symbol}")
                            continue

                    # Determine option type
                    option_type = 'Call' if symbol.startswith('C-') else 'Put'

                    # Get pricing and OI data
                    close_price = float(ticker.get('mark_price', 0) or 0)
                    
                    # FIXED: Use oi_contracts instead of oi for accurate contract count
                    oi_value = ticker.get('oi_contracts', 0) or 0
                    try:
                        open_interest = int(float(str(oi_value))) if oi_value else 0
                    except (ValueError, TypeError):
                        open_interest = 0

                    # FIXED: Use API timestamp instead of system time for accurate timing
                    if ticker.get('time'):
                        api_time = datetime.datetime.fromisoformat(ticker['time'].replace('Z', '+00:00'))
                        ist_time = api_time + datetime.timedelta(hours=5, minutes=30)
                        date_str = ist_time.strftime('%Y-%m-%d')
                        time_str = ist_time.strftime('%H:%M:%S')
                    else:
                        # Fallback to system time
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

                    # Log first few successful parses
                    if successful_parses <= 3:
                        logger.info(f"Successfully parsed #{successful_parses}: {symbol} -> Strike:{strike}, Close:{close_price}, OI:{open_interest}")

                except Exception as e:
                    failed_parses += 1
                    logger.info(f"Error parsing {symbol}: {e}")
                    if failed_parses <= 3:
                        import traceback
                        logger.info(f"Full error for {symbol}: {traceback.format_exc()}")
                    continue

        logger.info(f"Successful parses: {successful_parses}")
        logger.info(f"Failed parses: {failed_parses}")

        df = pd.DataFrame(eth_options)

        # FIXED: Enhanced duplicate removal using multiple columns
        df_unique = df.drop_duplicates(subset=['SYMBOL', 'Date', 'Time'], keep='last')

        # NEW: Sort by Expiry Date, Time, and Symbol for organized data
        df_unique_sorted = df_unique.sort_values(
            by=['Expiry_Date', 'Time', 'SYMBOL'], 
            ascending=[True, True, True]
        )

        logger.info(f"Collected {len(df)} ETH options records")
        logger.info(f"After removing duplicates and sorting: {len(df_unique_sorted)} unique records")

        return df_unique_sorted

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
        return df.tail(300)  # Get last 300 records for comparison

    except Exception as e:
        logger.error(f"Error getting previous data: {e}")
        return pd.DataFrame()

def calculate_open_and_oi_change(current_df, previous_df):
    """Calculate Open and OI_Change based on previous data"""
    if previous_df.empty:
        current_df['Open'] = ''
        current_df['OI_Change'] = ''
        return current_df

    # Convert to numeric for calculations
    previous_df['Close'] = pd.to_numeric(previous_df['Close'], errors='coerce')
    previous_df['OI'] = pd.to_numeric(previous_df['OI'], errors='coerce')

    # Merge current with previous data
    merged = current_df.merge(
        previous_df[['SYMBOL', 'Close', 'OI']],
        on='SYMBOL',
        how='left',
        suffixes=('', '_prev')
    )

    # Calculate Open (previous Close) and OI_Change
    merged['Open'] = merged['Close_prev'].fillna('')
    merged['OI_Change'] = (merged['OI'] - merged['OI_prev'].fillna(merged['OI'])).fillna('')

    # Set empty values for new symbols
    merged.loc[merged['Close_prev'].isna(), 'Open'] = ''
    merged.loc[merged['OI_prev'].isna(), 'OI_Change'] = ''

    # Keep only required columns in correct order
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
    logger.info("🚀 Starting ETH options data collection - FINAL VERSION")

    client = get_sheets_client()
    if not client:
        logger.error("Failed to initialize Google Sheets client")
        return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.sheet1

        # Fetch current ETH options data
        current_df = fetch_eth_options_data()

        if current_df.empty:
            logger.warning("No data collected")
            return

        # Get previous data for comparison
        previous_df = get_previous_data(worksheet)

        # Calculate Open and OI_Change fields
        final_df = calculate_open_and_oi_change(current_df, previous_df)

        # Append to Google Sheets
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
