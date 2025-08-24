import requests
import pandas as pd
import datetime
import gspread
from google.oauth2.service_account import Credentials
import logging
import numpy as np

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

def clean_dataframe_for_json(df):
    """Clean DataFrame to remove NaN and infinite values that cause JSON errors"""
    # Replace NaN, inf, -inf with None for JSON compatibility
    df_cleaned = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    
    # Also replace any remaining float NaN values
    df_cleaned = df_cleaned.where(pd.notnull(df_cleaned), None)
    
    return df_cleaned

def get_current_and_next_expiry(expiry_dates):
    """Get current and next expiry dates from a list of expiry dates"""
    try:
        unique_expiries = sorted(set(expiry_dates))
        current_date = datetime.date.today()
        
        current_expiry = None
        next_expiry = None
        
        for expiry in unique_expiries:
            if expiry >= current_date:
                if current_expiry is None:
                    current_expiry = expiry
                elif next_expiry is None and expiry > current_expiry:
                    next_expiry = expiry
                    break
        
        if current_expiry is None and unique_expiries:
            current_expiry = unique_expiries[-1]
            
        result = [current_expiry] if current_expiry else []
        if next_expiry:
            result.append(next_expiry)
            
        logger.info(f"🗓️ Current expiry: {current_expiry}, Next expiry: {next_expiry}")
        return result
        
    except Exception as e:
        logger.error(f"Error determining current/next expiry: {e}")
        return []

def filter_strikes_by_percentage(future_price, strike_price, percentage=7):
    """Check if strike is within ±percentage of future price"""
    lower_bound = future_price * (1 - percentage / 100)
    upper_bound = future_price * (1 + percentage / 100)
    return lower_bound <= strike_price <= upper_bound

def fetch_eth_options_data():
    """Fetch ETH options data using India Delta Exchange API"""
    try:
        logger.info("📡 Fetching ETH options from India Delta Exchange API...")
        
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

        # Get ETH spot price and calculate strike range
        eth_future_price = 0
        for ticker in tickers:
            if ticker.get('spot_price'):
                eth_future_price = float(ticker.get('spot_price'))
                break
        
        logger.info(f"💰 ETH Future Price: ${eth_future_price}")
        
        strike_lower = eth_future_price * 0.93  # -7%
        strike_upper = eth_future_price * 1.07  # +7%
        logger.info(f"🎯 Strike range filter: ${strike_lower:.2f} to ${strike_upper:.2f} (±7%)")
        
        # First pass: collect all expiry dates
        all_expiry_dates = []
        current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        
        for ticker in tickers:
            try:
                symbol = ticker.get('symbol', '')
                symbol_parts = symbol.split('-')
                if len(symbol_parts) >= 4:
                    expiry_str = symbol_parts[-1]
                    if len(expiry_str) == 6:
                        day = int(expiry_str[:2])
                        month = int(expiry_str[2:4])
                        year = 2000 + int(expiry_str[4:6])
                        expiry_date = datetime.date(year, month, day)
                        all_expiry_dates.append(expiry_date)
            except:
                continue
        
        # Get current and next expiry dates
        target_expiries = get_current_and_next_expiry(all_expiry_dates)
        if not target_expiries:
            logger.warning("⚠️ No valid expiry dates found")
            return pd.DataFrame()
        
        logger.info(f"🗓️ Filtering for expiries: {target_expiries}")

        # Second pass: process filtered options
        eth_options = []
        successful_parses = 0
        failed_parses = 0
        filtered_by_strike = 0

        for ticker in tickers:
            try:
                symbol = ticker.get('symbol', '')
                strike_price = ticker.get('strike_price')
                contract_type = ticker.get('contract_type', '')
                spot_price = ticker.get('spot_price')
                
                if not symbol or not strike_price or not contract_type or not spot_price:
                    failed_parses += 1
                    continue
                
                strike = float(strike_price)
                future_price = float(spot_price)
                
                # Filter by strike price range (±7%)
                if not filter_strikes_by_percentage(future_price, strike, 7):
                    filtered_by_strike += 1
                    continue
                
                # Parse expiry date
                symbol_parts = symbol.split('-')
                if len(symbol_parts) < 4:
                    failed_parses += 1
                    continue
                
                expiry_str = symbol_parts[-1]
                if len(expiry_str) == 6:
                    day = int(expiry_str[:2])
                    month = int(expiry_str[2:4])
                    year = 2000 + int(expiry_str[4:6])
                    expiry_date = datetime.date(year, month, day)
                else:
                    failed_parses += 1
                    continue
                
                # Filter by expiry
                if expiry_date not in target_expiries:
                    continue
                
                option_type = 'Call' if contract_type == 'call_options' else 'Put'
                mark_price = float(ticker.get('mark_price', 0))
                oi_contracts = int(ticker.get('oi_contracts', 0))

                option_data = {
                    'SYMBOL': symbol,
                    'Date': current_time.strftime('%Y-%m-%d'),
                    'Time': current_time.strftime('%H:%M:%S'),
                    'Future_Price': future_price,
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

                if successful_parses <= 5:
                    logger.info(f"✅ Parsed #{successful_parses}: {symbol} (Strike: {strike}, Expiry: {expiry_date})")

            except Exception as e:
                failed_parses += 1
                if failed_parses <= 3:
                    logger.warning(f"❌ Failed to parse {ticker.get('symbol', 'unknown')}: {e}")

        logger.info(f"📊 Results: {successful_parses} successful, {failed_parses} failed")
        logger.info(f"⚡ Filtered out {filtered_by_strike} options outside ±7% strike range")

        if successful_parses == 0:
            logger.error("💀 No ETH options were successfully parsed!")
            return pd.DataFrame()

        df = pd.DataFrame(eth_options)
        df_unique = df.drop_duplicates(subset=['SYMBOL'], keep='last')
        
        # Sort by Expiry Date, Time, and Symbol
        df_sorted = df_unique.sort_values(
            by=['Expiry_Date', 'Time', 'SYMBOL'], 
            ascending=[True, True, True]
        )
        
        logger.info(f"📋 Final dataset: {len(df_sorted)} ETH options (current + next expiry, ±7% strikes)")
        logger.info(f"📅 Expiries included: {sorted(df_sorted['Expiry_Date'].unique())}")
        logger.info(f"🎯 Strike range: ${df_sorted['Strike'].min():.0f} to ${df_sorted['Strike'].max():.0f}")
        return df_sorted

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
    merged.loc[merged['OI_prev'].isna(), 'OI_Change'] = None
    
    columns_order = ['SYMBOL', 'Date', 'Time', 'Future_Price', 'Expiry_Date', 
                    'Strike', 'Option_Type', 'Close', 'OI', 'Open', 'OI_Change']
    
    final_df = merged[columns_order].sort_values(
        by=['Expiry_Date', 'Time', 'SYMBOL'], 
        ascending=[True, True, True]
    )
    
    return final_df

def append_to_sheets(df, worksheet):
    """Append data to Google Sheets with proper data cleaning"""
    try:
        logger.info(f"📝 Attempting to append {len(df)} rows to Google Sheets...")
        
        # Clean DataFrame to fix JSON compliance issues
        df_cleaned = clean_dataframe_for_json(df)
        logger.info("🧹 Cleaned data for JSON compliance (removed NaN/inf values)")
        
        values = df_cleaned.values.tolist()
        
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
    logger.info("🚀 Starting ETH Options Data Collection - CLEANED & FILTERED VERSION")
    
    client = get_sheets_client()
    if not client:
        logger.error("Failed to initialize Google Sheets client")
        return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.sheet1

        current_df = fetch_eth_options_data()
        
        if current_df.empty:
            logger.warning("No ETH options data collected")
            return

        previous_df = get_previous_data(worksheet)
        final_df = calculate_open_and_oi_change(current_df, previous_df)
        
        logger.info(f"📊 Final data summary:")
        logger.info(f"   Rows: {len(final_df)}")
        logger.info(f"   Expiries: {sorted(final_df['Expiry_Date'].unique())}")
        logger.info(f"   Strike range: ${final_df['Strike'].min():.0f} to ${final_df['Strike'].max():.0f}")
        
        success = append_to_sheets(final_df, worksheet)
        
        if success:
            logger.info(f"🎉 SUCCESS: Updated {len(final_df)} ETH options (±7% strikes, current + next expiry)")
        else:
            logger.error("💀 FAILED: Could not update Google Sheets")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
