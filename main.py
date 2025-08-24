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
    """Fetch raw ETH options data with minimal processing"""
    try:
        url = "https://api.delta.exchange/v2/tickers"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        tickers = data.get('result', [])
        logger.info(f"Total tickers fetched from API: {len(tickers)}")

        eth_options = []
        successful_parses = 0
        failed_parses = 0

        # Get ETH spot price
        eth_price = 0
        for ticker in tickers:
            if ticker.get('symbol') == 'ETHUSD':
                eth_price = float(ticker.get('mark_price', 0) or 0)
                break
        logger.info(f"ETH price: {eth_price}")

        for ticker in tickers:
            symbol = ticker.get('symbol', '')

            if 'ETH' in symbol and (symbol.startswith('C-') or symbol.startswith('P-')):
                try:
                    # Basic parsing - no complex logic
                    strike_price = ticker.get('strike_price')
                    if strike_price:
                        strike = float(strike_price)
                    else:
                        parts = symbol.split('-')
                        if len(parts) < 4:
                            failed_parses += 1
                            continue
                        strike = float(parts[2])

                    # Simple expiry date processing
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

                    # RAW OI - no conversion, just log what we get
                    oi_contracts_raw = ticker.get('oi_contracts')
                    
                    # Log raw data for first 10 entries
                    if successful_parses < 10:
                        logger.info(f"RAW DATA {symbol}: oi_contracts_raw='{oi_contracts_raw}' (type: {type(oi_contracts_raw)})")

                    # Simple timestamp processing
                    api_time = datetime.datetime.fromisoformat(ticker['time'].replace('Z', '+00:00'))
                    date_str = (api_time + datetime.timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')
                    time_str = (api_time + datetime.timedelta(hours=5, minutes=30)).strftime('%H:%M:%S')

                    option_data = {
                        'SYMBOL': symbol,
                        'Date': date_str,
                        'Time': time_str,
                        'Future_Price': eth_price,
                        'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
                        'Strike': strike,
                        'Option_Type': option_type,
                        'Close': close_price,
                        'OI_RAW': oi_contracts_raw  # Keep raw value as-is
                    }

                    eth_options.append(option_data)
                    successful_parses += 1

                except Exception as e:
                    failed_parses += 1
                    if failed_parses <= 5:  # Log first 5 failures
                        logger.info(f"Error parsing {symbol}: {e}")
                    continue

        logger.info(f"Successful parses: {successful_parses}")
        logger.info(f"Failed parses: {failed_parses}")

        df = pd.DataFrame(eth_options)

        # Simple sorting only - no duplicate removal
        df_sorted = df.sort_values(by=['Expiry_Date', 'Time', 'SYMBOL'], ascending=[True, True, True])

        logger.info(f"Total records collected: {len(df_sorted)}")
        
        # Log some OI_RAW statistics to see what we have
        if not df_sorted.empty:
            logger.info(f"Sample OI_RAW values: {df_sorted['OI_RAW'].head(10).tolist()}")
            unique_oi_types = df_sorted['OI_RAW'].apply(type).value_counts()
            logger.info(f"OI_RAW data types: {unique_oi_types.to_dict()}")

        return df_sorted

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

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
    """Main data collection function - simplified"""
    logger.info("🚀 Starting simplified ETH options data collection - RAW DATA VERSION")

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

        # No Open/OI_Change calculation - just raw data
        success = append_to_sheets(current_df, worksheet)
        if success:
            logger.info(f"✅ Successfully collected and saved {len(current_df)} raw records")
        else:
            logger.error("❌ Failed to update Google Sheets")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
