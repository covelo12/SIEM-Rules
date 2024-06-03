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

# Create a new column to store the time difference between requests for each IP
data['time_diff'] = data.groupby('src_ip')['timestamp'].diff()

# Convert timestamp differences from units of 1/100th of a second to seconds
data['time_diff'] = data['time_diff'] / 100

# Calculate the average time difference for each IP
average_time_btw_requests = data.groupby('src_ip')['time_diff'].mean()

# Display the results (you can choose to display the entire DataFrame or specific columns)
small_data = average_time_btw_requests.nsmallest(20)


# Create a new column to store the time difference between requests for each IP
test['time_diff'] = test.groupby('src_ip')['timestamp'].diff()

# Convert timestamp differences from units of 1/100th of a second to seconds
test['time_diff'] = test['time_diff'] / 100

# Calculate the average time difference for each IP
average_time_btw_requests = test.groupby('src_ip')['time_diff'].mean()

# Display the results (you can choose to display the entire DataFrame or specific columns)
small_test = average_time_btw_requests.nsmallest(20)


# Merge the two dataframes on country code
merged_data = pd.merge(small_data, small_test, on='src_ip', suffixes=('_data', '_test'),  how='outer')
merged_data = merged_data.fillna(0)

print(merged_data.sort_values('time_diff_test'))