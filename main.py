import yfinance as yf
import pandas as pd
import time
import os


delisted = {'CBRG', 'GRRRW', 'FNVT', 'EMP', 'HPAC', 'AMPX-WT', 'LCFYW', 'SQFTW', 'SKYA', 'INTEW', 'AOGOW', 'HS', 'CYTO', 'LGL-WT', 'LGNYZ', 'VMCA', 'ME', 'OPLF', 'DECAW', 'DUETW', 'EDBLW', 'BZFDW', 'ORGNW', 'TOIIW', 'PETVW', 'MSSA', 'CELG-RI', 'PCG-PI', 'UWMC-WT', 'SHPW', 'CUENW', 'BTBDW', 'CRXT', 'KITTW', 'CNNB', 'LEXXW', 'CLRS', 'BWACW', 'ESH', 'ANGHW', 'TALKW', 'MAPSW', 'SEEL', 'BFRIW', 'CORZW', 'VMCAW', 'SKYH-WT', 'TLGY', 'PHUNW', 'PAVMZ', 'KIII', 'PUCKW', 'RENEU', 'RNLX', 'PRLHU', 'CFFSU', 'GWH-WT', 'AUROW', 'NVNOW', 'ARAC', 'BXT', 'AFMD', 'IVCBU', 'ALVOW', 'AFRIW', 'BC-PC', 'ADTHW', 'FHP', 'INVZW', 'AHPI', 'NRXPW', 'GGI', 'ATWO', 'SWVLW', 'ESGRP', 'WAVS', 'HCVIU', 'CFFSW', 'UNTCW', 'CMAX', 'PPYAW', 'FXCOW', 'CNTG', 'NNAVW', 'SLAM', 'EVGR', 'SNAXW', 'ALJJ', 'RENE', 'KIIIW', 'ADRU', 'IVCAW', 'MMAT', 'GBBK', 'NEOVW', 'SBP', 'NVVEW', 'PRLH', 'HAIAU', 'AACP', 'PROCW', 'MMV', 'CLRC', 'DECA', 'IVCB', 'FTE', 'ACONW', 'BRKH', 'NWAC', 'FNVTW', 'GFAIW', 'YOTAW', 'HAC', 'VERBW', 'CSLMU', 'AACIW', 'SRZNW', 'OXLCM', 'FICVW', 'ATLA', 'FGFPP', 'CINGW', 'NBSTW', 'ARKOW', 'BWAC', 'GMBLZ', 'PEGRU', 'CONN', 'IVCA', 'OXBRW', 'EVEX-WT', 'ASTLW', 'FTIIW', 'PSFE-WT', 'RIT', 'GCMGW', 'ZIVOW', 'DTSTW', 'LILM', 'BRSH', 'ICGA'}
not_yet_listed_2021 = {'HKIT', 'DRAY', 'DXF', 'GBUG', 'BGFDX', 'MLAC', 'CRCL', 'CEG', 'UBXG', 'SNPX', 'CAMP', 'HLNCF', 'BLBLF', 'NVCT', 'VWFB', 'SESG', 'KLC', 'APAC', 'PPLAF', 'SLVR', 'NIQ', 'RIV-PA', 'RRVCS', 'FEPI', 'SZZLU', 'LIPO', 'TGL', 'BLTE', 'ECCX', 'IONR', 'SOLS', 'JCSE', 'FEAM', 'FACT', 'BCHPY', 'FTII', 'DMYY', 'CKHUF', 'ZTOP', 'CPPTL', 'VRM', 'WRPT', 'ACLX', 'BNKD', 'FCNCO', 'LBGJ', 'CR', 'WEFCX', 'LNKB', 'MAIA', 'GRIN', 'MSPRW', 'NRGU', 'ARQQW', 'BHM', 'MOBBW', 'WTMA', 'TZUP', 'MS-PP', 'TFPM', 'SVRE', 'RESI', 'LVVR', 'DUET', 'MGAM', 'TKLF', 'TCMFF', 'INTS', 'MSGE', 'IXHL', 'STRW', 'GLE', 'GUER', 'AMLX', 'BTTC', 'PRH', 'GBTG', 'PNYG', 'RLTY', 'ELC', 'STRZ', 'SI', 'RVSN', 'INTM', 'VZLA', 'WLGS', 'UPYY', 'XGEIX', 'WCAP', 'GRFXF', 'AMPX', 'OST', 'LZGI'}

all_tickers = []
with open("data/raw/ticker.txt") as file:
    for line in file:
        ticker = line.split()[0].upper()
        if ticker not in delisted and ticker not in not_yet_listed_2021:
            all_tickers.append(ticker)

print(f"Total available tickers for download: {len(all_tickers)}")

START_DATE = "2021-01-01"
END_DATE = "2022-01-01"
BATCH_SIZE = 100  # 100 tickers per attempt
SLEEP_TIME = 10   # Delay between attempts (to prevent IP block for too many requests sent)
MAX_RETRIES = 5   # Max attempts for one package
OUTPUT_FILE = "data/raw/NASDAQ_FULL_2021_Close.csv"


def load_data_in_batches(all_tickers):
    """
    Loads tickers data with errors handling
    """
    downloaded_tickers = []

    if os.path.exists(OUTPUT_FILE):
        print(f"File {OUTPUT_FILE} already exists. Loading existing data.")
        try:
            existing_df = pd.read_csv(OUTPUT_FILE, index_col=0, parse_dates=True)
            downloaded_tickers = list(existing_df.columns)
            print(f"{len(downloaded_tickers)} tickers have been downloaded so far.")
        except Exception as e:
            print(f"Error reading existing file: {e}. Begging a new file.")
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()

    tickers_to_download = [t for t in all_tickers if t not in downloaded_tickers]
    print(f"{len(tickers_to_download)} tickers left to download.")

    for i in range(0, len(tickers_to_download), BATCH_SIZE):
        batch = tickers_to_download[i:i + BATCH_SIZE]
        attempt = 0
        success = False

        while attempt < MAX_RETRIES and not success:
            try:
                print(f"\n--- Loading package {i//BATCH_SIZE + 1} ({len(batch)} tickers). Tickers: {batch[0]}...{batch[-1]} ---")
                
                data = yf.download(
                    batch,
                    start=START_DATE,
                    end=END_DATE,
                    progress=False,
                )

                if data is not None and not data.empty:
                    data_close = data["Close"]
                    
                    if existing_df.empty:
                        existing_df = data_close
                    else:
                        existing_df = existing_df.join(data_close, how='outer')
                        
                    existing_df.to_csv(OUTPUT_FILE)
                    print(f"✅ Successfully downloaded and saved {len(batch)} tickers. Downloaded so far: {len(existing_df.columns)}")
                    success = True
                else:
                    print(f"⚠️ Yahoo Finance returned empty data for package. Skipping...")
                    success = True
            
            except Exception as e:
                attempt += 1
                if "Rate limited" in str(e):
                    print(f"🚨 Rate Limit Error. Attempt {attempt}/{MAX_RETRIES}. Waiting for {SLEEP_TIME * attempt} secs...")
                    time.sleep(SLEEP_TIME * attempt)
                else:
                    print(f"❌ Unknown error while loading package: {e}. Attempt {attempt}/{MAX_RETRIES}. Waiting for {SLEEP_TIME} secs...")
                    time.sleep(SLEEP_TIME)
        
        if not success:
            print(f"🔥 Failed to download package after {MAX_RETRIES} attempts. Last ticker: {batch[-1]}.")
            
        if success:
            print(f"Waiting for {SLEEP_TIME} secs before the next package...")
            time.sleep(SLEEP_TIME)

    print("\n\n=== 🎉 Download has completed! ===\n")
    print(f"Final file: {OUTPUT_FILE}")
    print(f"Total tickers got: {len(existing_df.columns)}")
    return existing_df

final_df = load_data_in_batches(all_tickers)
