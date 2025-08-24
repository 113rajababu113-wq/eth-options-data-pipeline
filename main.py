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
    """Fetch ETH options data - only the fields we need"""
    try:
        # Use tickers endpoint with ETH options filter
        url = "https://api.delta.exchange/v2/tickers"
        params = {
            'contract_types': 'call_options,put_options',
            'underlying_asset_symbols': 'ETH'
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        tickers = data.get('result', [])
        logger.info(f"Total ETH options tickers fetched: {len(tickers)}")

        eth_options = []
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        timestamp = int(current_time.timestamp())

        # Get ETH spot price
        spot_url = "https://api.delta.exchange/v2/tickers"
        spot_response = requests.get(spot_url, timeout=30)
        spot_data = spot_response.json()
        spot_tickers = spot_data.get('result', [])
        
        eth_spot_price = 0
        for ticker in spot_tickers:
            if ticker.get('symbol') == 'ETHUSD':
                eth_spot_price = float(ticker.get('spot_price', 0) or ticker.get('mark_price', 0) or 0)
                break
        
        logger.info(f"ETH spot price: {eth_spot_price}")

        # Process ETH options
        successful_parses = 0
        
        for ticker in tickers:
            symbol = ticker.get('symbol', '')
            contract_type = ticker.get('contract_type', '')
            
            # Skip if not ETH options
            if contract_type not in ['call_options', 'put_options'] or 'ETH' not in symbol:
                continue
            
            try:
                # Required fields from API
                strike_price = ticker.get('strike_price')
                if strike_price is None:
                    continue
                
                strike = float(strike_price)
                
                # Get expiry date
                expiry_str = ticker.get('expiry_date') or ticker.get('settlement_time')
                if not expiry_str:
                    continue
                
                # Parse expiry date
                if 'T' in expiry_str:
                    expiry_date = datetime.datetime.fromisoformat(expiry_str.replace('Z', '+00:00')).date()
                else:
                    expiry_date = datetime.datetime.strptime(expiry_str, '%Y-%m-%d').date()
                
                # Option type
                option_type = 'Call' if contract_type == 'call_options' else 'Put'
                
                # Get pricing data
                mark_price = float(ticker.get('mark_price', 0) or 0)
                
                # Get OI data
                oi_contracts = ticker.get('oi')
                if oi_contracts is None:
                    oi_contracts = 0
                else:
                    try:
                        oi_contracts = int(float(str(oi_contracts)))
                    except (ValueError, TypeError):
                        oi_contracts = 0

                # Create data matching your Google Sheets columns exactly
                option_data = {
                    'SYMBOL': symbol,
                    'Date': current_time.strftime('%Y-%m-%d'),
                    'Time': timestamp,  # Using timestamp as requested
                    'Future_Price': eth_spot_price,  # ETH spot price
                    'Expiry_Date': expiry_date.strftime('%Y-%m-%d'),
                    'Strike': strike,
                    'Option_Type': option_type,
                    'Close': mark_price,  # Using mark_price as Close
                    'OI': oi_contracts,   # Open interest contracts
                    'Open': '',           # Will be filled from previous data
                    'OI_Change': ''       # Will be calculated later
                }

                eth_options.append(option_data)
                successful_parses += 1

                # Log first few successful parses
                if successful_parses <= 5:
                    logger.info(f"Parsed #{successful_parses}: {symbol} - Strike:{strike}, Close:{mark_price}, OI:{oi_contracts}")

            except Exception as e:
                logger.warning(f"Error processing {symbol}: {e}")
                continue

        logger.info(f"Successfully parsed {successful_parses} ETH options")

        df = pd.DataFrame(eth_options)
        
        if not df.empty:
            # Remove duplicates
            df_unique = df.drop_duplicates(subset=['SYMBOL'], keep='last')
            logger.info(f"Final dataset: {len(df_unique)} unique ETH options")
            return df_unique
        else:
            logger.warning("No ETH options data found")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def get_previous_data(worksheet):
    """Get previous data from Google Sheets"""
    try:
        all_records = worksheet.get_all_records()
        if not all_records:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_records)
        return df.tail(300)  # Last 300 records
        
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
    
    # Calculate Open (previous Close price) and OI_Change
    merged['Open'] = merged['Close_prev'].fillna('')
    merged['OI_Change'] = (merged['OI'] - merged['OI_prev'].fillna(merged['OI'])).fillna('')
    
    # Set empty for new symbols
    merged.loc[merged['Close_prev'].isna(), 'Open'] = ''
    merged.loc[merged['OI_prev'].isna(), 'OI_Change'] = ''
    
    # Keep only the required columns in the exact order of your Google Sheets
    columns_order = ['SYMBOL', 'Date', 'Time', 'Future_Price', 'Expiry_Date', 
                    'Strike', 'Option_Type', 'Close', 'OI', 'Open', 'OI_Change']
    
    return merged[columns_order]

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
    logger.info("Starting ETH options data collection - SIMPLIFIED VERSION")
    
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
            logger.warning("No ETH options data collected")
            return

        # Get previous data for Open and OI_Change calculation
        previous_df = get_previous_data(worksheet)
        
        # Calculate Open and OI_Change
        final_df = calculate_open_and_oi_change(current_df, previous_df)
        
        # Log final data summary
        logger.info(f"Final dataset shape: {final_df.shape}")
        logger.info(f"Columns: {list(final_df.columns)}")
        
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
