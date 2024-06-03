import pandas as pd
import numpy as np
import dns.resolver
import dns.reversename
import pygeoip
import matplotlib.pyplot as plt 
import ipaddress


datafile = './../dataset/data6.parquet'

testfile = './../dataset/test6.parquet'
serverfile = './../dataset/servers6.parquet'


### IP geolocation
gi=pygeoip.GeoIP('./../GeoIP_DBs/GeoIP.dat')
gi2=pygeoip.GeoIP('./../GeoIP_DBs/GeoIPASNum.dat')

def isInternal(ip):
    if(ip.startswith("192")):
        return True
    return False 

def CountryFromIP(ip):
    return gi.country_code_by_addr(ip)

data = pd.read_parquet(datafile)
test = pd.read_parquet(testfile)
server = pd.read_parquet(serverfile)

# Filter data to include only internal source IP addresses
Num_internal_conns_data = data["dst_ip"].apply(lambda x:isInternal(x)).sum()
Num_internal_conns_test = test["dst_ip"].apply(lambda x:isInternal(x)).sum()


# Filter data to include only external source IP addresses
Num_external_conns_data = data['dst_ip'].apply(lambda x: not isInternal(x)).sum()
Num_external_conns_test = test['dst_ip'].apply(lambda x: not isInternal(x)).sum()


plt.figure(figsize=(12, 6))
plt.bar(['data', 'test'], [Num_internal_conns_data, Num_internal_conns_test])
plt.title('Number of Internal Connections')
plt.ylabel('Number of Connections')

plt.figure(figsize=(12, 6))
plt.bar(['data', 'test'], [Num_external_conns_data, Num_external_conns_test])
plt.title('Number of External Connections')
plt.ylabel('Number of Connections')

upS_data=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False).nlargest(10)
upS_test=data.loc[((test['port']==443))].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False).nlargest(10)
downS_data=data.loc[((data['port']==443))].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False).nlargest(10)
downS_test=data.loc[((test['port']==443))].groupby(['src_ip'])['up_bytes'].sum().sort_values(ascending=False).nlargest(10)

# Extract IPs present in both datasets
common_ips = upS_data.index.intersection(upS_test.index)
# Plotting
plt.figure(figsize=(12, 8))
bar_width = 0.35
index = np.arange(len(common_ips))
bar1 = plt.bar(index, upS_data, bar_width, color='blue', label='upS_data')
bar2 = plt.bar(index + bar_width, upS_test, bar_width, color='orange', label='upS_test')
plt.xlabel('Source IP')
plt.ylabel('Traffic')
plt.title('Traffic Comparison for Common IPs')
plt.xticks(index + bar_width / 2, common_ips, rotation=45)
plt.legend()
plt.tight_layout()


# Extract IPs present in both datasets
common_ips = downS_data.index.intersection(downS_test.index)
# Plotting
plt.figure(figsize=(12, 8))
bar_width = 0.35
index = np.arange(len(common_ips))
bar1 = plt.bar(index, downS_data, bar_width, color='blue', label='downS_data')
bar2 = plt.bar(index + bar_width, downS_test, bar_width, color='orange', label='downS_test')
plt.xlabel('Source IP')
plt.ylabel('Traffic')
plt.title('Traffic Comparison for Common IPs')
plt.xticks(index + bar_width / 2, common_ips, rotation=45)
plt.legend()
plt.tight_layout()
plt.show()  