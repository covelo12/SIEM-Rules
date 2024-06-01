import pandas as pd
import ipaddress
import pygeoip

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

# Is destination IPv4 a public address?
NET = ipaddress.IPv4Network('192.168.100.0/24')

size = int(len(data))
data = data[:size] # Limiting to first 2 rows for this example

# Check if dst_ip is not in the network
bpublic = data['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in NET)

data['country'] = data.loc[bpublic]['dst_ip'].drop_duplicates().apply(lambda y: gi.country_code_by_addr(y)).to_frame(name='cc')

# Group by country and sum the down_bytes for each country
country_traffic_data = data.groupby('country')['down_bytes'].sum()
print(country_traffic_data.nunique())


# Check if dst_ip is not in the network
bpublic = test['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in NET)

test['country'] = test.loc[bpublic]['dst_ip'].drop_duplicates().apply(lambda y: gi.country_code_by_addr(y)).to_frame(name='cc')

# Group by country and sum the down_bytes for each country
country_traffic_test = test.groupby('country')['down_bytes'].sum()
print(country_traffic_test.nunique())

# Merge the two dataframes on country code
merged_data = pd.merge(country_traffic_data, country_traffic_test, on='country', suffixes=('_data', '_test'),  how='outer')

# Fill missing values with 0
merged_data = merged_data.fillna(0)
# Convert the columns back to integer type
merged_data['down_bytes_data'] = merged_data['down_bytes_data'].astype(int)
merged_data['down_bytes_test'] = merged_data['down_bytes_test'].astype(int)

print(merged_data.nunique())
# Display the counts side by side
print(merged_data.nlargest(10, "down_bytes_test"))