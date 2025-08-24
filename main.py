import requests
import pandas as pd
import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SPREADSHEET_ID = '1YVJKTo8PDKLFqp7azkY1XhqizFRxY0GZB4RvSQe7KEA'
SERVICE_ACCOUNT_FILE = 'eth-options-key.json'

def get_sheets_client():
    """Initialize Google Sheets client"""
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
    except Exception as e:
        logger.error(f"❌ Error initializing sheets client: {e}")
        return None

def fetch_eth_options_data():
    """Fetch ETH options data using correct Delta Exchange API structure"""
    try:
        logger.info("📡 Fetching data from Delta Exchange API...")
        
        # Use the tickers endpoint with ETH options filter
        url = "https://api.delta.exchange/v2/tickers"
        params = {
            'contract_types': 'call_options,put_options',
            'underlying_asset_symbols': 'ETH'
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        tickers = data.get('result', [])
        logger.info(f"📈 Total tickers fetched with filters: {len(tickers)}")

        # Also try without filters to debug
        if len(tickers) == 0:
            logger.info("🔍 No results with filters, trying without filters...")
            response_all = requests.get(url, timeout=30)
            data_all = response_all.json()
            all_tickers = data_all.get('result', [])
            logger.info(f"📊 Total tickers without filters: {len(all_tickers)}")
            
            # Check what ETH-related tickers exist
            eth_tickers = [t for t in all_tickers if 'ETH' in t.get('symbol', '')]
            logger.info(f"🎯 ETH-related tickers found: {len(eth_tickers)}")
            
            if len(eth_tickers) > 0:
                # Show first few ETH tickers for debugging
                for i, ticker in enumerate(eth_tickers[:3]):
                    symbol = ticker.get('symbol', '')
                    contract_type = ticker.get('contract_type', '')
                    logger.info(f"Sample ETH ticker {i+1}: {symbol} - Contract type: {contract_type}")
            
            # Use all tickers for processing
            tickers = all_tickers

        # Get ETH spot price
        eth_spot_price = 0
        for ticker in tickers:
            if ticker.get('symbol') == 'ETHUSD':
                eth_spot_price = float(ticker.get('spot_price', 0) or ticker.get('mark_price', 0) or 0)
                break
        
        logger.info(f"💰 ETH spot price: ${eth_spot_price}")

        # Process ETH options data
        eth_options = []
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        timestamp = int(current_time.timestamp())
        
        successful_parses = 0
        failed_parses = 0

        for ticker in tickers:
            symbol = ticker.get('symbol', '')
            contract_type = ticker.get('contract_type', '')
            
            # Debug: Check what we're filtering against
            is_eth_related = 'ETH' in symbol
            is_option = contract_type in ['call_options', 'put_options']
            
            # Alternative check for options based on symbol pattern
            is_option_by_symbol = (symbol.startswith('C-ETH-') or symbol.startswith('P-ETH-'))
            
            # Use either contract_type OR symbol pattern
            if not (is_eth_related and (is_option or is_option_by_symbol)):
                continue
            
            try:
                # Get required fields
                strike_price = ticker.get('strike_price')
                if strike_price is None:
                    failed_parses += 1
                    continue
                
                strike = float(strike_price)
                
                # Get expiry date
                expiry_str = ticker.get('expiry_date') or ticker.get('settlement_time')
                if not expiry_str:
                    failed_parses += 1
                    continue
                
                # Parse expiry date
                if 'T' in expiry_str:
                    expiry_date = datetime.datetime.fromisoformat(expiry_str.replace('Z', '+00:00')).date()
                else:
                    expiry_date = datetime.datetime.strptime(expiry_str, '%Y-%m-%d').date()
                
                # Determine option type
                if contract_type == 'call_options' or symbol.startswith('C-'):
                    option_type = 'Call'
                elif contract_type == 'put_options' or symbol.startswith('P-'):
                    option_type = 'Put'
                else:
                    failed_parses += 1
                    continue
                
                # Get pricing and OI data
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

                # Log first few successful parses
                if successful_parses <= 5:
                    logger.info(f"✅ Parsed #{successful_parses}: {symbol} - Strike:{strike}, Close:{mark_price}, OI:{oi_contracts}")

            except Exception as e:
                failed_parses += 1
                if failed_parses <= 3:
                    logger.warning(f"❌ Failed to parse {symbol}: {e}")

        logger.info(f"📊 Results: {successful_parses} successful, {failed_parses} failed")

        if successful_parses == 0:
            logger.error("💀 No ETH options were successfully parsed!")
            return pd.DataFrame()

        df = pd.DataFrame(eth_options)
        df_unique = df.drop_duplicates(subset=['SYMBOL'], keep='last')
        
        logger.info(f"📋 Final dataset: {len(df_unique)} unique ETH options")
        return df_unique

    except Exception as e:
        logger.error(f"❌ Error fetching ETH options data: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return pd.DataFrame()

def get_previous_data(worksheet):
    """Get previous data from Google Sheets"""
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

    # Convert to numeric
    previous_df['Close'] = pd.to_numeric(previous_df['Close'], errors='coerce')
    previous_df['OI'] = pd.to_numeric(previous_df['OI'], errors='coerce')
    
    # Merge with previous data
    merged = current_df.merge(
        previous_df[['SYMBOL', 'Close', 'OI']],
        on='SYMBOL',
        how='left',
        suffixes=('', '_prev')
    )
    
    # Calculate Open and OI_Change
    merged['Open'] = merged['Close_prev'].fillna('')
    merged['OI_Change'] = (merged['OI'] - merged['OI_prev'].fillna(merged['OI'])).fillna('')
    
    # Set empty for new symbols
    merged.loc[merged['Close_prev'].isna(), 'Open'] = ''
    merged.loc[merged['OI_prev'].isna(), 'OI_Change'] = ''
    
    # Keep columns in the exact order of your Google Sheets
    columns_order = ['SYMBOL', 'Date', 'Time', 'Future_Price', 'Expiry_Date', 
                    'Strike', 'Option_Type', 'Close', 'OI', 'Open', 'OI_Change']
    
    return merged[columns_order]

def append_to_sheets(df, worksheet):
    """Append data to Google Sheets"""
    try:
        logger.info(f"📝 Attempting to append {len(df)} rows to Google Sheets...")
        values = df.values.tolist()
        
        result = worksheet.append_rows(values, value_input_option='USER_ENTERED')
        logger.info(f"✅ Successfully appended {len(values)} rows")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error appending to sheets: {e}")
        return False

def main():
    """Main data collection function"""
    logger.info("🚀 Starting ETH Options Data Collection - FIXED VERSION")
    
    client = get_sheets_client()
    if not client:
        logger.error("Failed to initialize Google Sheets client")
        return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.sheet1

        # Fetch ETH options data
        current_df = fetch_eth_options_data()
        
        if current_df.empty:
            logger.warning("No ETH options data collected")
            return

        # Get previous data for calculations
        previous_df = get_previous_data(worksheet)
        
        # Calculate Open and OI_Change
        final_df = calculate_open_and_oi_change(current_df, previous_df)
        
        # Append to Google Sheets
        success = append_to_sheets(final_df, worksheet)
        
        if success:
            logger.info(f"🎉 SUCCESS: Updated {len(final_df)} ETH options")
        else:
            logger.error("💀 FAILED: Could not update Google Sheets")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
