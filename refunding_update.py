#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3 #PULLS IT FROM DB FOR CORRECT DATE
import csv
import os

# Specify the directory path
directory_path = r"J:\Python\ksm"

# Combine the directory path with the database file name
db_file_path = os.path.join(directory_path, "ksm.db")

# Connect to SQLite database
conn = sqlite3.connect(db_file_path)
cursor = conn.cursor()

# Execute SQL query to retrieve data with date formatted as 'YYYY-MM-DD'
query = """
    SELECT 
        *
    FROM taxexempt
    WHERE date = (SELECT MAX(date) FROM taxexempt);
"""


cursor.execute(query)

# Commit changes to the database
conn.commit()

# Fetch the column names
column_names = [description[0] for description in cursor.description]

# Fetch the results
data = cursor.fetchall()

# Specify the output CSV file path, creates a new csv with data, not formatted yet
output_csv_path = os.path.join(directory_path, 'refunding', 'output_data.csv')

# Export data to CSV with column names
with open(output_csv_path, 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    
    # Write the header (column names)
    csv_writer.writerow(column_names)
    
    # Write the data
    csv_writer.writerows(data)

# Close the database connection
conn.close()


# In[ ]:


import os
import pandas as pd

# Replace 'path/to/taxexempt.csv' with the actual path to your CSV file
csv_path = os.path.join(directory_path, 'refunding', 'output_data.csv')
df = pd.read_csv(csv_path)

# Change the scale column to the corresponding year
df['year'] = 2023 + df['scale']

# Keep only relevant columns
df = df[['date', 'year', 'rating', 'value']]

# Convert the date column to the 'YYYY-MM-DD' format
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

# Pivot the DataFrame using pivot_table
df_pivot = df.pivot_table(index=['date', 'year'], columns='rating', values='value', aggfunc='mean')

# Reset the index to make 'year' a column again
df_pivot.reset_index(inplace=True)

# Select columns using the loc accessor
df_pivot = df_pivot.loc[:, ['date', 'year', 'AAA', 'AA','A', 'BAA']]


# Specify the output CSV file path for the modified data
modified_csv_path = os.path.join(directory_path, 'refunding', 'modified_output_data.csv')

# Export modified data to CSV
df_pivot.to_csv(modified_csv_path, index=False)


# View the result
df_pivot.head()


# In[ ]:


import csv

# Specify the paths to the CSV files
master_csv_path = os.path.join(directory_path, 'refunding', 'master_data.csv')
modified_csv_path = os.path.join(directory_path, 'refunding', 'modified_output_data.csv')

# Read the modified CSV data excluding the header
with open(modified_csv_path, 'r') as modified_file:
    modified_reader = csv.reader(modified_file)
    next(modified_reader)  # Skip the header
    modified_data = list(modified_reader)

# Read the existing master CSV data
with open(master_csv_path, 'r') as master_file:
    master_reader = csv.reader(master_file)
    master_data = list(master_reader)

# Find the index of the last row with data in the master CSV
last_data_index = next((i for i, row in enumerate(master_data[::-1]) if any(cell.strip() for cell in row)), None)

# Concatenate the modified data to the master data
result_data = master_data[:-(last_data_index + 1)] + modified_data

# Write the combined data back to the master CSV file
with open(master_csv_path, 'w', newline='') as master_file:
    master_writer = csv.writer(master_file)
    master_writer.writerows(result_data)

