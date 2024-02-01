#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import requests
import logging
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

LOGIN_URL = 'https://www.tm3.com/homepage/login.jsf'
CUSIP_EVAL_URL_TEMPLATE = 'https://www.tm3.com/mvsearch/cusipEvalHistoryContent.jsf?cusipId={}'
USERNAME = 'REDACTED'
PASSWORD = 'REDACTED'

DB_FILE = 'mmd.db'

def create_database_table(cursor): #with conn for a bit of  data integrity??
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS evaluation_history (
            cusip TEXT,
            date TEXT,
            price TEXT,
            category TEXT
        )
    ''')

def login(session):
    payload = {}
    result = session.get(LOGIN_URL, verify=False)
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

def scrape_cusip_data(cusip_id, category, session, cursor, conn):
    base_url = CUSIP_EVAL_URL_TEMPLATE.format(cusip_id)

    logger.info(f'Logging in before scraping CUSIP {cusip_id} at {base_url}...')
    logger.debug(f'Base URL: {base_url}')

    login(session)

    logger.info(f'Scraping CUSIP {cusip_id} at {base_url}...')
    response = session.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    config2a_div = soup.find('div', class_='config2a')
    groupA_div = config2a_div.find('div', class_='groupA') if config2a_div else None
    group_a_table = groupA_div.find('table', class_='data') if groupA_div else None

    # Initialize date and price with default values, maybe do a choose 5 to try and get it weekly?
    date = 'N/A'
    price = '0'

    if group_a_table:
        # Extract the first row (most recent date) if it exists
        row = group_a_table.select_one('tbody tr')
        if row:
            cells = row.find_all('td')
            date = cells[0].get_text(strip=True)
            price = cells[1].get_text(strip=True)

    # If the date is 'N/A', use the most recent weekday date
    if date == 'N/A':
        today = datetime.today()
        # Calculate the most recent weekday date (excluding Saturday and Sunday)
        if today.weekday() == 0:  # Monday
            date = (today - timedelta(days=3)).strftime('%m/%d/%Y')
        else:
            date = (today - timedelta(days=1)).strftime('%m/%d/%Y')

    # Insert the data into the database
    cursor.execute('''
        INSERT INTO evaluation_history (cusip, date, price, category)
        VALUES (?, ?, ?, ?)
    ''', (cusip_id, date, price, category)) #SQL parametization helps protect against unwanted code injections

    conn.commit()



def main():
    with requests.Session() as session:
        logging.info('Opening session...')
        session.mount('https://', HTTPAdapter(max_retries=3))
        session.verify = False  # Set to True if SSL verification is required, might for better security (longer runtime)

        # Establish a connection and create a cursor
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        create_database_table(cursor)

        csv_directory = 'J:/Python/ksm/ksm2023/CSV'

        for filename in os.listdir(csv_directory):
            if filename.endswith('.csv'):
                category = os.path.splitext(filename)[0]
                csv_file_path = os.path.join(csv_directory, filename)

                cusips_to_scrape = pd.read_csv(csv_file_path, usecols=['CUSIP'], index_col='CUSIP').index.tolist()

                for cusip in cusips_to_scrape:
                    logging.info(f'Scraping CUSIP {cusip} in category {category}...')
                    scrape_cusip_data(cusip, category, session, cursor, conn)

        # Close the connection after processing
        conn.close()

    logging.info('Done...')

if __name__ == '__main__':
    main()

