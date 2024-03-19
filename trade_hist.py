#!/usr/bin/env python
# coding: utf-8

# In[3]:


import os
import requests
import logging
import sqlite3
import pandas as pd
import re
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

# Setup basic logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

# URLs for the CUSIP data
LOGIN_URL = 'https://www.tm3.com/homepage/login.jsf'
CUSIP_EVAL_URL_TEMPLATE = 'https://www.tm3.com/mvsearch/cusipEvalHistoryContent.jsf?cusipId={}'
CUSIP_TRADE_URL_TEMPLATE = 'https://www.tm3.com/mvsearch/cusipTradeHistoryContent.jsf?cusipId={}'

# Credentials for login
USERNAME = 'REDACTED'
PASSWORD = 'REDACTED'

# Database file path
DB_FILE = 'mmd.db'

def create_database_table(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS refund_history (
            cusip TEXT,
            series TEXT,
            date TEXT,
            price TEXT,
            trade_date TEXT,
            num_trades INTEGER,
            total_par REAL,
            high_price REAL,
            low_price REAL,
            avg_px_buy REAL,
            avg_yld_buy REAL,
            avg_px_sell REAL,
            avg_yld_sell REAL,
            avg_px_intra REAL,
            avg_yld_intra REAL
        )
    ''')

def login(session):
    payload = {}
    result = session.get(LOGIN_URL, verify=True)
    soup = BeautifulSoup(result.content, 'html.parser')
    
    hidden_inputs = soup.find_all('input', type='hidden')
    for hidden_input in hidden_inputs:
        name = hidden_input.get('name')
        payload[name] = hidden_input.get('value')
    
    payload['username'] = USERNAME
    payload['password'] = PASSWORD
    payload['loginButton'] = 'Login'
    
    logger.debug('Payload is %s' % payload)
    logger.info('Attempting to login...')
    
    login_response = session.post(LOGIN_URL, data=payload, timeout=30.0)
    
    if "Invalid login" in login_response.text:
        raise Exception("Login failed. Check credentials.")
    else:
        logger.info("Login successful.")
        
def validate_cusip(cusip):
    if not re.match(r'^[0-9A-Za-z]{9}$', cusip):
        raise ValueError('Invalid CUSIP format')

def scrape_cusip_data(cusip, series, session, cursor, num_dates=1):
    base_url = CUSIP_EVAL_URL_TEMPLATE.format(cusip)
    logger.info(f'Scraping CUSIP {cusip} at {base_url}...')
    response = session.get(base_url)
    logger.debug(f'Response status code: {response.status_code}')
    
    soup = BeautifulSoup(response.text, 'html.parser')
    config2a_div = soup.find('div', class_='config2a')
    groupA_div = config2a_div.find('div', class_='groupA') if config2a_div else None
    group_a_table = groupA_div.find('table', class_='data') if groupA_div else None

    if group_a_table:
        rows = group_a_table.select('tbody tr')[:num_dates]
        for row in rows:
            cells = row.find_all('td')
            date = cells[0].get_text(strip=True)
            price = cells[1].get_text(strip=True)
            trade_data = scrape_trade_data(cusip, series, session)
            # Check if trade_data is not None before proceeding
            if trade_data:
                cursor.execute('''
                    INSERT INTO refund_history (cusip, series, date, price, trade_date, num_trades, total_par, high_price, low_price,
                                                avg_px_buy, avg_yld_buy, avg_px_sell, avg_yld_sell, avg_px_intra, avg_yld_intra)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (cusip, series, date, price, trade_data['trade_date'], trade_data['num_trades'], trade_data['total_par'],
                      trade_data['high_price'], trade_data['low_price'], trade_data['avg_px_buy'], trade_data['avg_yld_buy'],
                      trade_data['avg_px_sell'], trade_data['avg_yld_sell'], trade_data['avg_px_intra'], trade_data['avg_yld_intra']))
            else:
                # Handle the case when trade_data is None (e.g., log an error, skip the record, or use default values)
                logger.error(f"Failed to obtain trade data for CUSIP {cusip}.")

        
def scrape_trade_data(cusip, series, session):
    trade_history_url = CUSIP_TRADE_URL_TEMPLATE.format(cusip)
    response = session.get(trade_history_url)
    if response.status_code != 200:
        logger.error(f"Failed to fetch trade data for CUSIP {cusip}. HTTP Status Code: {response.status_code}")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')

    # Locating the trade history table
    trade_history_table = None
    for table in soup.find_all('table'):
        if 'Trade Date' in str(table):
            trade_history_table = table
            break
    
    if not trade_history_table:
        logger.error(f"No trade history table found for CUSIP {cusip}.")
        return None

    first_row = trade_history_table.find('tbody').find('tr')
    cells = first_row.find_all('td')
    
    if cells:
        return parse_trade_row(cusip, series, cells)
    else:
        logger.error(f"Failed to parse trade data for CUSIP {cusip}.")
        return None

def parse_trade_row(cusip, series, cells):
    try:
        # Skipping the 'W/I' column and including 'series'
        trade_data = {
            'cusip': cusip,
            'series': series,
            'trade_date': cells[0].text.strip(),
            'num_trades': int(cells[1].text.strip()),
            'total_par': float(cells[3].text.replace(',', '').strip()),
            'high_price': float(cells[4].text.strip()),
            'low_price': float(cells[5].text.strip()),
            'avg_px_buy': float(cells[6].text.split('/')[0].strip()),
            'avg_yld_buy': float(cells[6].text.split('/')[1].strip()) if '/' in cells[6].text else None,
            'avg_px_sell': float(cells[7].text.split('/')[0].strip()),
            'avg_yld_sell': float(cells[7].text.split('/')[1].strip()) if '/' in cells[7].text else None,
            'avg_px_intra': float(cells[8].text.split('/')[0].strip()),
            'avg_yld_intra': float(cells[8].text.split('/')[1].strip()) if '/' in cells[8].text else None,
        }
        return trade_data
    except Exception as e:
        logger.error(f"Error parsing trade row: {e}")
        return None
    
def main():
    with requests.Session() as session:
        logging.info('Opening session...')
        session.mount('https://', HTTPAdapter(max_retries=3))

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        create_database_table(cursor)

        csv_file_path = 'J:/Python/ksm/ksm2023/CSV/test.csv'
        df = pd.read_csv(csv_file_path)  # Read the entire CSV into a DataFrame
        
        login(session)  # Login once per session

        for index, row in df.iterrows():
            cusip = row['CUSIP']
            series = row['SERIES']  # Adjust this based on your actual column name for series
            logging.info(f'Scraping CUSIP {cusip} with series {series}...')
            scrape_cusip_data(cusip, series, session, cursor)  # Pass 'series' to your scraping function

        conn.commit()  # Commit all changes at once
        conn.close()

    logging.info('Done...')

if __name__ == '__main__':
    main()

