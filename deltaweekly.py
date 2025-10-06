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

def get_current_and_next_friday_expiry(expiry_dates):
    """Get W1 (using count >= 2 rule) and W2 (nearest Friday after W1)"""
    try:
        unique_expiries = sorted(set(expiry_dates))
        current_date = datetime.date.today()
        
        logger.info(f"📅 Available expiry dates: {unique_expiries}")
        
        # Filter only active expiries (>= current date)
        active_expiries = [exp for exp in unique_expiries if exp >= current_date]
        
        if not active_expiries:
            logger.warning("⚠️ No active expiry dates found")
            return []
        
        # Find Friday expiries from active expiries  
        friday_expiries = [exp for exp in active_expiries if exp.weekday() == 4]
        friday_expiries.sort()
        
        logger.info(f"🗓️ Available Friday expiries: {friday_expiries}")
        
        if not friday_expiries:
            logger.warning("⚠️ No Friday expiries found")
            return []
        
        # STEP 1: Find W1 using existing count >= 2 rule (UNCHANGED)
        w1_expiry = None
        for friday_exp in friday_expiries:
            expiries_before_friday = [exp for exp in active_expiries if exp < friday_exp]
            count = len(expiries_before_friday)
            
            logger.info(f"🎯 Friday {friday_exp}: {count} expiries before it")
            
            if count >= 2:
                w1_expiry = friday_exp
                logger.info(f"✅ W1 (Current weekly): {w1_expiry}")
                break
            else:
                logger.info(f"❌ Count ({count}) < 2 → SKIP {friday_exp}")
        
        # Fallback for W1
        if not w1_expiry:
            w1_expiry = friday_expiries[0]
            logger.warning(f"⚠️ Using fallback W1: {w1_expiry}")
        
        # STEP 2: Find W2 = Nearest Friday after W1
        w2_expiry = None
        for friday_exp in friday_expiries:
            if friday_exp > w1_expiry:  # First Friday after W1
                w2_expiry = friday_exp
                logger.info(f"✅ W2 (Next weekly): {w2_expiry}")
                break
        
        # Build result
        result_expiries = [w1_expiry]
        if w2_expiry:
            result_expiries.append(w2_expiry)
        else:
            logger.warning(f"⚠️ No Friday found after W1")
        
        logger.info(f"🎯 Final selected expiries: {result_expiries}")
        logger.info(f"   W1: {result_expiries[0]}")
        logger.info(f"   W2: {result_expiries[1] if len(result_expiries) > 1 else 'Not found'}")
        
        return result_expiries
        
    except Exception as e:
        logger.error(f"Error determining Friday expiries: {e}")
        return []



def filter_strikes_by_percentage(future_price, strike_price, percentage=25):
    """Check if strike is within ±percentage of future price"""
    lower_bound = future_price * (1 - percentage / 100)
    upper_bound = future_price * (1 + percentage / 100)
    return lower_bound <= strike_price <= upper_bound

def fetch_eth_options_data():
    """Fetch ETH options data using India Delta Exchange API"""
    try:
        logger.info("📡 Fetching ETH weekly options from India Delta Exchange API...")
        
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
        
        strike_lower = eth_future_price * 0.75  # -25%
        strike_upper = eth_future_price * 1.25  # +25%
        logger.info(f"🎯 Strike range filter: ${strike_lower:.2f} to ${strike_upper:.2f} (±25%)")

        
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
        target_expiries = get_current_and_next_friday_expiry(all_expiry_dates)
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
                
                # Filter by strike price range (±25%)
                if not filter_strikes_by_percentage(future_price, strike, 25):
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
                    'Open': 0,        # Initialize as 0
                    'OI_Change': 0    # Initialize as 0
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
        logger.info(f"⚡ Filtered out {filtered_by_strike} options outside ±25% strike range")

        if successful_parses == 0:
            logger.error("💀 No ETH weekly options were successfully parsed!")
            return pd.DataFrame()

        df = pd.DataFrame(eth_options)
        df_unique = df.drop_duplicates(subset=['SYMBOL'], keep='last')
        
        # Sort by Expiry Date, Time, and Symbol
        df_sorted = df_unique.sort_values(
            by=['Expiry_Date', 'Time', 'SYMBOL'], 
            ascending=[True, True, True]
        )
        
        logger.info(f"📋 Final dataset: {len(df_sorted)} ETH weekly options (Friday expiry, ±25% strikes)")
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
    """Calculate Open and OI_Change based on previous data - FIXED to avoid row duplication"""
    if previous_df.empty:
        # No previous data - set Open and OI_Change to 0
        current_df['Open'] = 0
        current_df['OI_Change'] = 0
        logger.info("🆕 No previous data found - setting Open and OI_Change to 0")
        return current_df

    # Convert to numeric for calculations
    previous_df['Close'] = pd.to_numeric(previous_df['Close'], errors='coerce')
    previous_df['OI'] = pd.to_numeric(previous_df['OI'], errors='coerce')
    
    # Create a lookup dictionary from previous data for faster processing
    previous_lookup = {}
    for _, row in previous_df.iterrows():
        symbol = row['SYMBOL']
        previous_lookup[symbol] = {
            'Close': row['Close'] if pd.notnull(row['Close']) else 0,
            'OI': row['OI'] if pd.notnull(row['OI']) else 0
        }
    
    logger.info(f"📚 Created lookup for {len(previous_lookup)} previous symbols")
    
    # Calculate Open and OI_Change for current data
    open_values = []
    oi_change_values = []
    
    for _, row in current_df.iterrows():
        symbol = row['SYMBOL']
        current_oi = row['OI']
        
        if symbol in previous_lookup:
            # Symbol exists in previous data
            prev_close = previous_lookup[symbol]['Close']
            prev_oi = previous_lookup[symbol]['OI']
            
            open_values.append(prev_close)
            oi_change_values.append(current_oi - prev_oi)
        else:
            # New symbol - no previous data
            open_values.append(0)
            oi_change_values.append(0)
    
    # Assign calculated values to current dataframe
    current_df['Open'] = open_values
    current_df['OI_Change'] = oi_change_values
    
    # Ensure proper column order
    columns_order = ['SYMBOL', 'Date', 'Time', 'Future_Price', 'Expiry_Date', 
                    'Strike', 'Option_Type', 'Close', 'OI', 'Open', 'OI_Change']
    
    # Final sort by Expiry Date, Time, and Symbol
    final_df = current_df[columns_order].sort_values(
        by=['Expiry_Date', 'Time', 'SYMBOL'], 
        ascending=[True, True, True]
    ).reset_index(drop=True)
    
    # Log calculation summary
    new_symbols = len(current_df) - len([s for s in current_df['SYMBOL'] if s in previous_lookup])
    existing_symbols = len(current_df) - new_symbols
    
    logger.info(f"🔄 Calculated Open/OI_Change: {existing_symbols} existing, {new_symbols} new symbols")
    
    return final_df

def append_to_sheets(df, worksheet):
    """Append data to Google Sheets with proper data cleaning"""
    try:
        logger.info(f"📝 Attempting to append {len(df)} rows to Google Sheet2...")
        
        # Clean DataFrame to fix JSON compliance issues
        df_cleaned = clean_dataframe_for_json(df)
        logger.info("🧹 Cleaned data for JSON compliance (removed NaN/inf values)")
        
        values = df_cleaned.values.tolist()
        
        result = worksheet.append_rows(values, value_input_option='USER_ENTERED')
        logger.info(f"✅ Successfully appended {len(values)} rows to Sheet2")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error appending to sheets: {e}")
        import traceback
        logger.error(f"Full error: {traceback.format_exc()}")
        return False

def main():
    """Main data collection function"""
    logger.info("🚀 Starting ETH Weekly Options Data Collection - FIXED DUPLICATION & 0 VALUES")
    
    client = get_sheets_client()
    if not client:
        logger.error("Failed to initialize Google Sheets client")
        return

    try:
        sheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = sheet.worksheet('Sheet2')

        # Fetch current ETH options data
        current_df = fetch_eth_options_data()
        
        if current_df.empty:
            logger.warning("No ETH options data collected")
            return

        # Get previous data for Open and OI_Change calculations
        previous_df = get_previous_data(worksheet)
        
        # Calculate Open and OI_Change - FIXED VERSION
        final_df = calculate_open_and_oi_change(current_df, previous_df)
        
        # Log final data summary
        logger.info(f"📊 Final data summary:")
        logger.info(f"   Rows: {len(final_df)}")
        expiry_list = sorted(final_df['Expiry_Date'].unique())
        logger.info(f"   Expiries: {expiry_list}")
        logger.info(f"   W1 Expiry: {expiry_list[0] if len(expiry_list) > 0 else 'None'}")
        logger.info(f"   W2 Expiry: {expiry_list[1] if len(expiry_list) > 1 else 'None'}")
        logger.info(f"   Strike range: ${final_df['Strike'].min():.0f} to ${final_df['Strike'].max():.0f}")


        
        # Append to Google Sheets
        success = append_to_sheets(final_df, worksheet)
        
        if success:
            expiry_count = len(target_expiries)
            logger.info(f"🎉 SUCCESS: Updated {len(final_df)} ETH options ({expiry_count} expiries: W1+W2)")
        else:
            logger.error("💀 FAILED: Could not update Google Sheets")


    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()





