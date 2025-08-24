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
    """Fetch ETH options data using India Delta Exchange API"""
    try:
        logger.info("📡 Fetching ETH options from India Delta Exchange API...")
        
        # Use India Delta Exchange API endpoint with ETH options filter
        url = "https://api.india.delta.exchange/v2/tickers"
        params = {
            'contract_types': 'call_options,put_options',
            'underlying_asset_symbols': 'ETH'
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        tickers = data.get('result', [])
        logger.info(f"📊 Total ETH options fetched: {len(tickers)}")

        if len(tickers) == 0:
            logger.warning("⚠️ No ETH options found in API response")
            return pd.DataFrame()

        # Process ETH options
        eth_options = []
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        
        successful_parses = 0
        failed_parses = 0

        for ticker in tickers:
            try:
                # Get required fields based on India Delta Exchange API response
                symbol = ticker.get('symbol', '')
                strike_price = ticker.get('strike_price')
                contract_type = ticker.get('contract_type', '')
                spot_price = ticker.get('spot_price')
                
                # Skip if missing required fields
                if not symbol or not strike_price or not contract_type or not spot_price:
                    failed_parses += 1
                    continue
                
                strike = float(strike_price)
                future_price = float(spot_price)
                
                # Parse expiry date from symbol (P-ETH-4820-240825 -> 240825 = 24 Aug 2025)
                symbol_parts = symbol.split('-')
                if len(symbol_parts) < 4:
                    failed_parses += 1
                    continue
                
                expiry_str = symbol_parts[-1]  # e.g., "240825"
                if len(expiry_str) == 6:
                    # Parse DDMMYY format - where YY is 20XX
                    day = int(expiry_str[:2])    # 24
                    month = int(expiry_str[2:4]) # 08  
                    year = 2000 + int(expiry_str[4:6])  # 2025
                    expiry_date = datetime.date(year, month, day)
                else:
                    failed_parses += 1
                    continue
                
                # Determine option type
                option_type = 'Call' if contract_type == 'call_options' else 'Put'
                
                # Get pricing and OI data
                mark_price = float(ticker.get('mark_price', 0))
                oi_contracts = int(ticker.get('oi_contracts', 0))

                option_data = {
                    'SYMBOL': symbol,
                    'Date': current_time.strftime('%Y-%m-%d'),
                    'Time': current_time.strftime('%H:%M:%S'),  # Readable time format
                    'Future_Price': future_price,                # Using spot_price from API
                    'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
                    'Strike': strike,
                    'Option_Type': option_type,
                    'Close': mark_price,
                    'OI': oi_contracts,
                    'Open': '',           # Will be filled from previous data
                    'OI_Change': ''       # Will be calculated later
                }

                eth_options.append(option_data)
                successful_parses += 1

                # Log first few successful parses
                if successful_parses <= 5:
                    logger.info(f"✅ Parsed #{successful_parses}: {symbol}")
                    logger.info(f"   Strike: {strike}, Future Price: {future_price}, Close: {mark_price}, OI: {oi_contracts}")

            except Exception as e:
                failed_parses += 1
                if failed_parses <= 3:
                    logger.warning(f"❌ Failed to parse {ticker.get('symbol', 'unknown')}: {e}")

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
    
    # Merge current data with previous data
    merged = current_df.merge(
        previous_df[['SYMBOL', 'Close', 'OI']],
        on='SYMBOL',
        how='left',
        suffixes=('', '_prev')
    )
    
    # Calculate Open (previous Close price) and OI_Change
    merged['Open'] = merged['Close_prev'].fillna('')
    merged['OI_Change'] = (merged['OI'] - merged['OI_prev'].fillna(merged['OI'])).fillna('')
    
    # Set empty values for new symbols (no previous data)
    merged.loc[merged['Close_prev'].isna(), 'Open'] = ''
    merged.loc[merged['OI_prev'].isna(), 'OI_Change'] = ''
    
    # Keep columns in exact order matching your Google Sheets
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
        import traceback
        logger.error(f"Full error: {traceback.format_exc()}")
        return False

def main():
    """Main data collection function"""
    logger.info("🚀 Starting ETH Options Data Collection - FINAL VERSION")
    
    client = get_sheets_client()
    if not client:
        logger.error("Failed to initialize Google Sheets client")
        return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.sheet1

        # Fetch ETH options data from India Delta Exchange
        current_df = fetch_eth_options_data()
        
        if current_df.empty:
            logger.warning("No ETH options data collected")
            return

        # Get previous data for Open and OI_Change calculations
        previous_df = get_previous_data(worksheet)
        
        # Calculate Open (previous close) and OI_Change
        final_df = calculate_open_and_oi_change(current_df, previous_df)
        
        # Log final data summary
        logger.info(f"📊 Final data summary:")
        logger.info(f"   Rows: {len(final_df)}")
        logger.info(f"   Columns: {list(final_df.columns)}")
        if not final_df.empty:
            sample_row = final_df.iloc[0].to_dict()
            logger.info(f"   Sample: {sample_row}")
        
        # Append to Google Sheets
        success = append_to_sheets(final_df, worksheet)
        
        if success:
            logger.info(f"🎉 SUCCESS: Updated {len(final_df)} ETH options in Google Sheets")
        else:
            logger.error("💀 FAILED: Could not update Google Sheets")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
