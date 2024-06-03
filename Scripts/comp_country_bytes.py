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

data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: gi.country_code_by_addr(y)).to_frame(name='cc')
test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: gi.country_code_by_addr(y)).to_frame(name='cc')

# Group by country and sum the down_bytes for each country
country_traffic_data = data.groupby('country')['down_bytes'].sum()
country_traffic_test = test.groupby('country')['down_bytes'].sum()

# Merge the two dataframes on country code
merged_data = pd.merge(country_traffic_data, country_traffic_test, on='country', suffixes=('_data', '_test'),  how='outer')
merged_data = merged_data.fillna(0)

# Convert the columns back to integer type
merged_data['down_bytes_data'] = merged_data['down_bytes_data'].astype(int)
merged_data['down_bytes_test'] = merged_data['down_bytes_test'].astype(int)

top_countries = merged_data.nlargest(10, "down_bytes_test")

# Plotting
plt.figure(figsize=(14, 7))
bar_width = 0.35
index = range(len(top_countries))
plt.bar(index, top_countries['down_bytes_data'], bar_width, label='Data')
plt.bar([i + bar_width for i in index], top_countries['down_bytes_test'], bar_width, label='Test')
plt.xlabel('Country')
plt.ylabel('Down Bytes')
plt.title('Down Bytes by Country (Top 10)')
plt.xticks([i + bar_width / 2 for i in index], top_countries.index)
plt.legend()
plt.tight_layout()
plt.savefig("img/down_bytes_countrys_10_largest.png")

####################### Upload

# Group by country and sum the down_bytes for each country
country_traffic_data = data.groupby('country')['up_bytes'].sum()
country_traffic_test = test.groupby('country')['up_bytes'].sum()

# Merge the two dataframes on country code
merged_data = pd.merge(country_traffic_data, country_traffic_test, on='country', suffixes=('_data', '_test'),  how='outer')
merged_data = merged_data.fillna(0)

# Convert the columns back to integer type
merged_data['up_bytes_data'] = merged_data['up_bytes_data'].astype(int)
merged_data['up_bytes_test'] = merged_data['up_bytes_test'].astype(int)

top_countries = merged_data.nlargest(10, "up_bytes_test")

# Plotting
plt.figure(figsize=(14, 7))
bar_width = 0.35
index = range(len(top_countries))
plt.bar(index, top_countries['up_bytes_data'], bar_width, label='Data')
plt.bar([i + bar_width for i in index], top_countries['up_bytes_test'], bar_width, label='Test')
plt.xlabel('Country')
plt.ylabel('Up Bytes')
plt.title('Up Bytes by Country (Top 10)')
plt.xticks([i + bar_width / 2 for i in index], top_countries.index)
plt.legend()
plt.tight_layout()
plt.savefig("img/up_bytes_countrys_10_largest.png")
plt.show()
