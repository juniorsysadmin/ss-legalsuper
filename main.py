import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

BROWSING_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Referer": "https://investments.legalsuper.com.au/super/investment-performance/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
}

ENDPOINT = "https://investments.legalsuper.com.au/super/investment-performance/downloadPerformanceDataCSV/"
SUPER_FUND_NAME = "Legal Super Pty Ltd"


def process_csv_data(csv_content: str, output_dir: Path) -> None:
    """Process the CSV data and save individual fund files."""
    # Create output directory
    output_dir.mkdir(exist_ok=True)

    # Read CSV, skipping first 3 lines
    df = pd.read_csv(StringIO(csv_content), skiprows=3)

    # Convert Date from DD/MM/YYYY to YYYYMMDD
    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y").dt.strftime("%Y%m%d")

    # Extract fund names from column headers
    # Columns are named like "MySuper Balanced (Buy Price)" and "MySuper Balanced (Sell Price)"
    fund_names = set()
    for col in df.columns:
        if col != "Date":
            # Extract fund name by removing " (Buy Price)" or " (Sell Price)"
            match = re.match(r"(.+) \((Buy|Sell) Price\)", col)
            if match:
                fund_name = match.group(1)
                fund_names.add(fund_name)

    print(f"Found {len(fund_names)} funds to process")

    # Process each fund
    for i, fund_name in enumerate(sorted(fund_names)):
        print(f"[{i+1}/{len(fund_names)}] Processing: {fund_name}")

        buy_col = f"{fund_name} (Buy Price)"
        sell_col = f"{fund_name} (Sell Price)"

        # Check if both columns exist
        if buy_col in df.columns and sell_col in df.columns:
            # Create dataframe with Date, Buy, Sell columns
            fund_df = pd.DataFrame(
                {"Date": df["Date"], "Buy": df[buy_col], "Sell": df[sell_col]}
            )

            # Remove rows where both Buy and Sell are NaN
            fund_df = fund_df.dropna(subset=["Buy", "Sell"], how="all")

            # Remove rows where both Buy and Sell are 0.0
            fund_df = fund_df[~((fund_df["Buy"] == 0.0) & (fund_df["Sell"] == 0.0))]

            # Where Sell is 0.0, set it to the same value as Buy
            fund_df.loc[fund_df["Sell"] == 0.0, "Sell"] = fund_df.loc[
                fund_df["Sell"] == 0.0, "Buy"
            ]

            # Sort by Date in ascending order
            fund_df = fund_df.sort_values(by="Date")

            # Save to CSV
            output_path = output_dir / f"{fund_name}.csv"
            fund_df.to_csv(output_path, index=False)
            print(
                f"  ✓ Successfully processed and saved: {output_path} ({len(fund_df)} rows)"
            )
        else:
            print(f"  ✗ Missing Buy or Sell column for {fund_name}")

    print(f"\nProcessing complete. Files saved to {output_dir}/ directory")


# Main execution
if __name__ == "__main__":
    print(f"Fetching {ENDPOINT}...")

    # Send GET request to fetch the CSV data
    response = requests.get(url=ENDPOINT, headers=BROWSING_HEADERS, timeout=25)

    # Check if the request was successful
    if response.status_code == 200:
        print("CSV file successfully downloaded")

        # Process the CSV data
        csv_content = response.content.decode("utf-8")
        output_dir = Path(SUPER_FUND_NAME)
        process_csv_data(csv_content, output_dir)
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
