import pandas as pd
import ipaddress
import pygeoip
import matplotlib.pyplot as plt 

# Load your data
datafile = './../dataset/data6.parquet'
testfile = './../dataset/test6.parquet'
serverfile = './../dataset/servers6.parquet'

# IP geolocation
gi = pygeoip.GeoIP('./../GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./../GeoIP_DBs/GeoIPASNum.dat')

# Read data
data = pd.read_parquet(datafile)
test = pd.read_parquet(testfile)
server = pd.read_parquet(serverfile)

up_data = data.groupby('src_ip')['up_bytes'].sum().nlargest(10)
up_test = test.groupby('src_ip')['up_bytes'].sum().nlargest(10)

# Merge the two dataframes on country code
merged_data = pd.merge(up_data, up_test, on='src_ip', suffixes=('_data', '_test'),  how='outer')
merged_data = merged_data.fillna(0)

# Convert the columns back to integer type
merged_data['up_bytes_data'] = merged_data['up_bytes_data'].astype(int)
merged_data['up_bytes_test'] = merged_data['up_bytes_test'].astype(int)

print(merged_data)

down_data = data.groupby('src_ip')['down_bytes'].sum().nlargest(10)
down_test = test.groupby('src_ip')['down_bytes'].sum().nlargest(10)

# Merge the two dataframes on country code
merged_data = pd.merge(down_data, down_test, on='src_ip', suffixes=('_data', '_test'),  how='outer')
merged_data = merged_data.fillna(0)

# Convert the columns back to integer type
merged_data['down_bytes_data'] = merged_data['down_bytes_data'].astype(int)
merged_data['down_bytes_test'] = merged_data['down_bytes_test'].astype(int)

print(merged_data)