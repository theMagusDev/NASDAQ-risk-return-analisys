import pandas as pd
import requests
import time
import os

API_KEY = "2370151fbfa5393f393b21928686ef4cdd42fd3e01db408559fb26c32b25fe1b"
BASE_URL = "https://api.sec-api.io/mapping/ticker/"
INPUT_FILE = "data/processed/NASDAQ_FULL_2021_Cleaned_Imputed.csv"
OUTPUT_FILE = "data/processed/NASDAQ_Company_Details.csv"
BATCH_SIZE = 50           # Number of tickers to process before saving progress
SLEEP_TIME = 0.5          # Delay in seconds between individual API calls (Crucial for Rate Limits)
MAX_RETRIES = 3           # Max attempts for a single ticker

try:
    data = pd.read_csv(INPUT_FILE, index_col=0, parse_dates=True)
    all_tickers = data.columns.tolist()
    if 'Date' in all_tickers:
        all_tickers.remove('Date')
    all_tickers = [ticker.upper() for ticker in all_tickers]
    print(f"Total available tickers loaded from {INPUT_FILE}: {len(all_tickers)}")

except FileNotFoundError:
    print(f"Error: Input file not found at {INPUT_FILE}. Please check the path.")
    exit()
except Exception as e:
    print(f"An error occurred while loading the input file: {e}")
    exit()

def get_company_details(ticker, api_key):
    """
    Executes a GET request to the SEC Mapping API for a single ticker.
    [cite_start]Uses ^ and $ for exact ticker match[cite: 14, 15].
    """
    # [cite_start]The API endpoint structure: https://api.sec-api.io/mapping/ticker/<TICKER> [cite: 9]
    # [cite_start]We use ^ and $ for exact matching as per documentation [cite: 14, 15]
    endpoint = f"{BASE_URL}^{ticker}$?token={api_key}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(endpoint)
            response.raise_for_status()

            data = response.json() 

            if data and isinstance(data, list):
                # The API returns a list of results; take the first one for exact match
                details = data[0]
                details['requestedTicker'] = ticker 
                return details
            else:
                # This often happens if the ticker is not found in the SEC database
                print(f"   Ticker {ticker} not found or no data returned.")
                return None
            
        except requests.exceptions.HTTPError as e:
            # Handle API errors (e.g., Rate Limit, 404, etc.)
            if response.status_code == 429: # Too Many Requests
                 print(f"   🚨 Rate Limit hit for {ticker}. Attempt {attempt + 1}/{MAX_RETRIES}. Waiting {SLEEP_TIME * 5} sec...")
                 time.sleep(SLEEP_TIME * 5)
            else:
                print(f"   ❌ HTTP Error for {ticker} ({response.status_code}): {e}. Skipping.")
                return None
                
        except Exception as e:
            print(f"   ❌ Unknown error requesting {ticker}: {e}. Skipping.")
            return None
        
        # Delay between retries
        time.sleep(SLEEP_TIME)
        
    print(f"   🔥 Failed to get data for {ticker} after {MAX_RETRIES} attempts.")
    return None

if os.path.exists(OUTPUT_FILE):
    try:
        company_df = pd.read_csv(OUTPUT_FILE, index_col='requestedTicker')
        downloaded_tickers = set(company_df.index.tolist())
        print(f"Output file {OUTPUT_FILE} exists. {len(downloaded_tickers)} companies already downloaded.")
    except Exception as e:
        print(f"Error reading existing output file: {e}. Starting fresh.")
        company_df = pd.DataFrame()
        downloaded_tickers = set()
else:
    company_df = pd.DataFrame()
    downloaded_tickers = set()

tickers_to_download = [t for t in all_tickers if t not in downloaded_tickers]
print(f"Remaining tickers to download details for: {len(tickers_to_download)}")

results_list = []
total_downloaded_count = len(downloaded_tickers)

for i, ticker in enumerate(tickers_to_download):
    
    print(f"\n--- [{i + 1}/{len(tickers_to_download)}] Requesting ticker: {ticker} ---")
    
    # Get company details
    details = get_company_details(ticker, API_KEY)
    
    if details:
        results_list.append(details)
        total_downloaded_count += 1
        print(f"   ✅ Successfully retrieved. Session count: {len(results_list)}. Total overall: {total_downloaded_count}")

    time.sleep(SLEEP_TIME) 
    
    if len(results_list) >= BATCH_SIZE:
        print(f"\n*** Saving batch of {len(results_list)} companies... ***")
        
        new_df = pd.DataFrame(results_list).set_index('requestedTicker')
        
        company_df = pd.concat([company_df, new_df])
        
        company_df.to_csv(OUTPUT_FILE)
        
        results_list = []
        print("*** Batch saved. Continuing... ***")


if results_list:
    print(f"\n*** Final save of remaining {len(results_list)} companies... ***")
    new_df = pd.DataFrame(results_list).set_index('requestedTicker')
    company_df = pd.concat([company_df, new_df])

company_df.to_csv(OUTPUT_FILE)

print("\n\n=== 🎉 Company metadata download complete! ===")
print(f"Final output file: {OUTPUT_FILE}")
print(f"Total companies in file: {len(company_df)}")
