#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime

def create_csv(input_excel_path, output_csv_path):
    # Read the Excel file into a Pandas DataFrame
    df = pd.read_excel(input_excel_path)

    # Get the most recent weekday's date
    today = datetime.date.today()
    offset = max(1, (today.weekday() + 6) % 7 - 3)
    most_recent_weekday = today - datetime.timedelta(days=offset)

    # Update the dates in the Bloomberg formulas
    df['Date'] = most_recent_weekday.strftime('%m/%d/%Y')

    # Replace empty values with 'N/A'
    df = df.fillna('N/A')
    
    # Save the DataFrame to the existing CSV file (overwriting it)
    df.to_csv(output_csv_path, index=False)

# Specify the paths
input_excel_path = r"J:\Python\ksm\refunding\BVAL.xlsx"
output_csv_path = r"J:\Python\ksm\refunding\BVAL.csv"

# Call the function to create a new CSV file (overwriting the previous one)
create_csv(input_excel_path, output_csv_path)


# In[ ]:




