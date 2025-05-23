import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

def log_progress(message):
    """Log progress messages to code_log.txt with timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open('code_log.txt', 'a') as log_file:
        log_file.write(f'{timestamp}: {message}\n')

def extract():
    """Extract bank data from Wikipedia"""
    log_progress("Starting data extraction")
    
    # URL for web scraping
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    
    try:
        # Get webpage content
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the table under 'By market capitalization'
        table = soup.find('table', {'class': 'wikitable sortable mw-collapsible'})
        
        # Extract data
        data = []
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows[:10]:  # Get top 10 banks
            cols = row.find_all('td')
            if len(cols) >= 2:
                name = cols[0].text.strip()
                mc_usd = float(cols[1].text.strip().replace(',', ''))
                data.append([name, mc_usd])
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=['Name', 'MC_USD_Billion'])
        
        log_progress("Data extraction completed successfully")
        return df
    
    except Exception as e:
        log_progress(f"Error during extraction: {str(e)}")
        raise

def transform(df):
    """Transform DataFrame by adding market cap in GBP, EUR, and INR"""
    log_progress("Starting data transformation")
    
    try:
        # Read exchange rates
        exchange_rates = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv')
        
        # Convert exchange rates to dictionary
        rates = {
            'GBP': float(exchange_rates[exchange_rates['Currency'] == 'GBP']['Rate']),
            'EUR': float(exchange_rates[exchange_rates['Currency'] == 'EUR']['Rate']),
            'INR': float(exchange_rates[exchange_rates['Currency'] == 'INR']['Rate'])
        }
        
        # Add new columns with converted values
        df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * rates['GBP']).round(2)
        df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * rates['EUR']).round(2)
        df['MC_INR_Billion'] = (df['MC_USD_Billion'] * rates['INR']).round(2)
        
        log_progress("Data transformation completed successfully")
        return df
    
    except Exception as e:
        log_progress(f"Error during transformation: {str(e)}")
        raise

def load_to_csv(df):
    """Save DataFrame to CSV file"""
    log_progress("Starting CSV file creation")
    
    try:
        df.to_csv('./Largest_banks_data.csv', index=False)
        log_progress("Data successfully saved to CSV")
    except Exception as e:
        log_progress(f"Error saving to CSV: {str(e)}")
        raise

def load_to_db(df):
    """Save DataFrame to SQLite database"""
    log_progress("Starting database loading")
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('Banks.db')
        df.to_sql('Largest_banks', conn, if_exists='replace', index=False)
        conn.close()
        log_progress("Data successfully loaded to database")
    except Exception as e:
        log_progress(f"Error loading to database: {str(e)}")
        raise

def run_queries():
    """Run verification queries on the database"""
    log_progress("Starting database queries")
    
    try:
        conn = sqlite3.connect('Banks.db')
        cursor = conn.cursor()
        
        # Query 1: Display all records
        print("\nAll Records:")
        cursor.execute("SELECT * FROM Largest_banks")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
        # Query 2: Display average market cap in USD
        print("\nAverage Market Cap in USD:")
        cursor.execute("SELECT AVG(MC_USD_Billion) FROM Largest_banks")
        avg_usd = cursor.fetchone()[0]
        print(f"{avg_usd:.2f}")
        
        # Query 3: Display banks with market cap > 100 billion USD
        print("\nBanks with Market Cap > 100 billion USD:")
        cursor.execute("SELECT Name, MC_USD_Billion FROM Largest_banks WHERE MC_USD_Billion > 100")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
        conn.close()
        log_progress("Database queries completed successfully")
    except Exception as e:
        log_progress(f"Error running queries: {str(e)}")
        raise

def main():
    """Main function to execute the ETL pipeline"""
    log_progress("Starting ETL pipeline")
    
    # Extract
    df = extract()
    print("\nExtracted Data:")
    print(df)
    
    # Transform
    df_transformed = transform(df)
    print("\nTransformed Data:")
    print(df_transformed)
    
    # Load to CSV
    load_to_csv(df_transformed)
    
    # Load to Database
    load_to_db(df_transformed)
    
    # Run Queries
    run_queries()
    
    log_progress("ETL pipeline completed successfully")

if __name__ == "__main__":
    main()