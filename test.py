 

import sqlite3

import pandas as pd

# Create a new SQLite database
conn = sqlite3.connect('shipment_database.db')
c = conn.cursor()



##read the spreadsheets into dataframes of pandas
df0 = pd.read_csv('/data/shipping_data_0.csv')
df1 = pd.read_csv('/data/shipping_data_1.csv')
df2 = pd.read_csv('/data/shipping_data_2.csv')

#insert the data from the first spreadsheet into the database
df0.to_sql('product', conn, if_exists='append', index=False)

# Process and insert the data from spreadsheets 1 and 2 into the database
# This will depend on your specific database schema and the structure of the spreadsheets

# Group df1 by shipping identifier and calculate the quantity of goods in each shipment
df1_grouped = df1.groupby('shipping_identifier').agg({'product': list, 'product': 'count'}).reset_index()
df1_grouped.columns = ['shipping_identifier', 'products', 'quantity']

# Merge df1_grouped with df2
df_merged = pd.merge(df1_grouped, df2, on='shipping_identifier')

# Insert the merged data into the database
df_merged.to_sql('shipment', conn, if_exists='append', index=False)