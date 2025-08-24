import requests
import pandas as pd
import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging
import traceback

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SPREADSHEET_ID = '1YVJKTo8PDKLFqp7azkY1XhqizFRxY0GZB4RvSQe7KEA'
SERVICE_ACCOUNT_FILE = 'eth-options-key.json'

def get_sheets_client():
    """Initialize Google Sheets client with enhanced error handling"""
    try:
        logger.info("🔑 Initializing Google Sheets client...")
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
        client = gspread.authorize(creds)
        logger.info("✅ Google Sheets client initialized successfully")
        return client
    except FileNotFoundError:
        logger.error("❌ Service account file 'eth-options-key.json' not found")
        return None
    except Exception as e:
        logger.error(f"❌ Error initializing sheets client: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return None

def test_sheets_connection(client):
    """Test connection to Google Sheets"""
    try:
        logger.info("🔗 Testing Google Sheets connection...")
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.sheet1
        
        # Get basic info
        all_records = worksheet.get_all_records()
        current_rows = len(all_records)
        
        logger.info(f"✅ Successfully connected to sheet")
        logger.info(f"📊 Current rows in sheet: {current_rows}")
        return worksheet, current_rows
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Google Sheets: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return None, 0

def fetch_eth_options_data():
    """Fetch ETH options data with enhanced debugging"""
    try:
        logger.info("📡 Fetching ETH options data from Delta Exchange...")
        
        # Test basic API connectivity first
        url = "https://api.delta.exchange/v2/tickers"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        tickers = data.get('result', [])
        logger.info(f"📈 Total tickers fetched: {len(tickers)}")

        # Count ETH options specifically
        eth_options_count = 0
        eth_calls = 0
        eth_puts = 0
        
        for ticker in tickers:
            symbol = ticker.get('symbol', '')
            contract_type = ticker.get('contract_type', '')
            
            if 'ETH' in symbol and contract_type in ['call_options', 'put_options']:
                eth_options_count += 1
                if contract_type == 'call_options':
                    eth_calls += 1
                else:
                    eth_puts += 1

        logger.info(f"🎯 Found {eth_options_count} ETH options ({eth_calls} calls, {eth_puts} puts)")

        if eth_options_count == 0:
            logger.warning("⚠️ No ETH options found in API response")
            return pd.DataFrame()

        # Get ETH spot price
        eth_spot_price = 0
        for ticker in tickers:
            if ticker.get('symbol') == 'ETHUSD':
                eth_spot_price = float(ticker.get('spot_price', 0) or ticker.get('mark_price', 0) or 0)
                break
        
        logger.info(f"💰 ETH spot price: ${eth_spot_price}")

        # Process options data
        eth_options = []
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        timestamp = int(current_time.timestamp())
        
        successful_parses = 0
        failed_parses = 0

        for ticker in tickers:
            symbol = ticker.get('symbol', '')
            contract_type = ticker.get('contract_type', '')
            
            if 'ETH' not in symbol or contract_type not in ['call_options', 'put_options']:
                continue
            
            try:
                # Get required fields
                strike_price = ticker.get('strike_price')
                if strike_price is None:
                    failed_parses += 1
                    continue
                
                strike = float(strike_price)
                
                # Get expiry
                expiry_str = ticker.get('expiry_date') or ticker.get('settlement_time')
                if not expiry_str:
                    failed_parses += 1
                    continue
                
                if 'T' in expiry_str:
                    expiry_date = datetime.datetime.fromisoformat(expiry_str.replace('Z', '+00:00')).date()
                else:
                    expiry_date = datetime.datetime.strptime(expiry_str, '%Y-%m-%d').date()
                
                option_type = 'Call' if contract_type == 'call_options' else 'Put'
                mark_price = float(ticker.get('mark_price', 0) or 0)
                
                oi_contracts = ticker.get('oi', 0)
                try:
                    oi_contracts = int(float(str(oi_contracts))) if oi_contracts else 0
                except:
                    oi_contracts = 0

                option_data = {
                    'SYMBOL': symbol,
                    'Date': current_time.strftime('%Y-%m-%d'),
                    'Time': timestamp,
                    'Future_Price': eth_spot_price,
                    'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
                    'Strike': strike,
                    'Option_Type': option_type,
                    'Close': mark_price,
                    'OI': oi_contracts,
                    'Open': '',
                    'OI_Change': ''
                }

                eth_options.append(option_data)
                successful_parses += 1

            except Exception as e:
                failed_parses += 1
                if failed_parses <= 3:  # Log first few failures
                    logger.warning(f"Failed to parse {symbol}: {e}")

        logger.info(f"✅ Successfully parsed: {successful_parses}")
        logger.info(f"❌ Failed to parse: {failed_parses}")

        if successful_parses == 0:
            logger.warning("⚠️ No options were successfully parsed")
            return pd.DataFrame()

        df = pd.DataFrame(eth_options)
        df_unique = df.drop_duplicates(subset=['SYMBOL'], keep='last')
        
        logger.info(f"📋 Final dataset: {len(df_unique)} unique ETH options")
        return df_unique

    except Exception as e:
        logger.error(f"❌ Error fetching ETH options data: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return pd.DataFrame()

def append_to_sheets_safe(df, worksheet):
    """Safely append data with detailed error reporting"""
    try:
        logger.info(f"📝 Attempting to append {len(df)} rows to Google Sheets...")
        
        if len(df) == 0:
            logger.warning("⚠️ No data to append")
            return False
        
        # Convert to values
        values = df.values.tolist()
        logger.info(f"🔄 Converted to {len(values)} rows for upload")
        
        # Show sample of data being uploaded
        if len(values) > 0:
            logger.info(f"📋 Sample row: {values[0][:5]}...")  # First 5 columns
        
        # Attempt the upload
        result = worksheet.append_rows(values, value_input_option='USER_ENTERED')
        
        logger.info(f"✅ Successfully appended {len(values)} rows")
        logger.info(f"📊 API response: {result}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error appending to sheets: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False

def main():
    """Main function with comprehensive debugging"""
    logger.info("🚀 Starting ETH Options Data Collection - DEBUG VERSION")
    logger.info(f"🕐 Current time: {datetime.datetime.now()}")
    
    # Step 1: Initialize Google Sheets
    client = get_sheets_client()
    if not client:
        logger.error("💀 Cannot proceed without Google Sheets client")
        return

    # Step 2: Test connection
    worksheet, current_rows = test_sheets_connection(client)
    if not worksheet:
        logger.error("💀 Cannot proceed without worksheet connection")
        return

    # Step 3: Fetch ETH options data
    current_df = fetch_eth_options_data()
    
    if current_df.empty:
        logger.error("💀 No ETH options data to process")
        return

    # Step 4: Process and upload
    logger.info(f"🔄 Processing {len(current_df)} options for upload...")
    
    success = append_to_sheets_safe(current_df, worksheet)
    
    if success:
        logger.info(f"🎉 SUCCESS: Updated {len(current_df)} ETH options")
    else:
        logger.error("💀 FAILED: Could not update Google Sheets")

    logger.info("🏁 ETH Options Data Collection completed")

if __name__ == "__main__":
    main()
