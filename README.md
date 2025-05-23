# Bank Market Capitalization ETL Pipeline

## Project Overview
This project is a Python-based ETL (Extract, Transform, Load) pipeline developed as part of a Coursera course. It extracts data on the top 10 largest banks in the world by market capitalization from Wikipedia, transforms the data by converting market capitalization into GBP, EUR, and INR, and loads the results into a CSV file and a SQLite database. The pipeline includes logging functionality to track progress and sample SQL queries to verify the data.

## Features
- **Extraction**: Scrapes tabular data from Wikipedia using BeautifulSoup.
- **Transformation**: Converts market capitalization from USD to GBP, EUR, and INR using exchange rates from a CSV file.
- **Loading**: Saves the transformed data to a CSV file (`Largest_banks_data.csv`) and a SQLite database (`Banks.db`).
- **Logging**: Records progress at each stage in `code_log.txt`.
- **Querying**: Executes sample SQL queries to demonstrate database functionality.

## Prerequisites
To run this project, you need:
- Python 3.6+
- Required libraries:
  ```bash
  pip install pandas requests beautifulsoup4
  ```
- Internet access to fetch the Wikipedia page and exchange rate CSV.

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/BankMarketCapETL.git
   cd BankMarketCapETL
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   (Create a `requirements.txt` with `pandas`, `requests`, and `beautifulsoup4` if desired.)

## Usage
1. Run the script:
   ```bash
   python banks_project.py
   ```
2. The script will:
   - Extract data from the specified Wikipedia URL.
   - Transform the data using exchange rates.
   - Save the output to `Largest_banks_data.csv` and `Banks.db`.
   - Log progress to `code_log.txt`.
   - Execute sample SQL queries and print results.

## File Structure
- `banks_project.py`: Main Python script containing the ETL pipeline.
- `code_log.txt`: Log file with timestamped progress messages (generated on run).
- `Largest_banks_data.csv`: Output CSV file with transformed data (generated on run).
- `Banks.db`: SQLite database file with the `Largest_banks` table (generated on run).

## Data Sources
- Wikipedia: [List of largest banks](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)
- Exchange Rates: [CSV file](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv)

## Sample Output
The script produces a CSV file and database table with the following columns:
- `Name`: Bank name
- `MC_USD_Billion`: Market capitalization in USD
- `MC_GBP_Billion`: Market capitalization in GBP
- `MC_EUR_Billion`: Market capitalization in EUR
- `MC_INR_Billion`: Market capitalization in INR

Example queries include:
- Displaying all records
- Calculating the average market cap in USD
- Listing banks with market cap > $100 billion

## License
This project is licensed under the MIT License.

## Acknowledgments
This project was completed as part of a Coursera course on data engineering with Python.
