import pandas as pd
import numpy as np
import dns.resolver
import dns.reversename
import pygeoip
import matplotlib.pyplot as plt 
import ipaddress


datafile = './../dataset/data6.parquet'


### IP geolocation
gi=pygeoip.GeoIP('./GeoIP.dat')
gi2=pygeoip.GeoIP('./GeoIPASNum.dat')

def isInternal(ip):
    if(ip.startswith("192")):
        return True
    return False 

def CountryFromIP(ip):
    return gi.country_code_by_addr(ip)

data = pd.read_parquet(datafile)

# Filter data to include only internal source IP addresses
Num_internal_conns = data["dst_ip"].apply(lambda x:isInternal(x)).sum()

# Filter data to include only external source IP addresses
Num_external_conns = data['dst_ip'].apply(lambda x: not isInternal(x)).sum()

# Filter data for port 443 and group by port and protocol
bytes_per_port = data.groupby(['proto'])[['up_bytes', 'down_bytes']].sum()

# Filter data for port 443 and group by port and protocol
bytes_per_port_443 = data[data['port'] == 443].groupby(['proto'])[['up_bytes', 'down_bytes']].sum()

# Filter data for port 53 and group by port and protocol
bytes_per_port_53 = data[data['port'] == 53].groupby(['proto'])[['up_bytes', 'down_bytes']].sum()

# Filter by ip
bytes_per_ip = data.groupby('src_ip')[['up_bytes', 'down_bytes']].sum().nlargest(10, 'up_bytes')

# Filter by country_connections
data['country'] = data["src_ip"].apply(lambda x:CountryFromIP(x))
bytes_per_country = data.groupby('country')[['up_bytes', 'down_bytes']].sum()

###############################
## Plot the data for port ##
###############################
plt.figure(figsize=(12, 6))
bytes_per_port.plot(kind='bar', stacked=True)
plt.title('Total Bytes Transferred for Port 443 by Protocol')
plt.xlabel('Protocol')
plt.ylabel('Bytes')
plt.legend(['Uploaded Bytes', 'Downloaded Bytes'])
plt.tight_layout()
plt.savefig('bytes_per_portocol.png')

###############################
## Plot the data for port 443 ##
###############################
plt.figure(figsize=(12, 6))
bytes_per_port_443.plot(kind='bar', stacked=True)
plt.title('Total Bytes Transferred for Port 443 by Protocol')
plt.xlabel('Protocol')
plt.ylabel('Bytes')
plt.legend(['Uploaded Bytes', 'Downloaded Bytes'])
plt.tight_layout()
plt.savefig('bytes_per_port_443.png')

##############################
## Plot the data for port 53 ##
##############################
plt.figure(figsize=(12, 6))
bytes_per_port_53.plot(kind='bar', stacked=True)
plt.title('Total Bytes Transferred for Port 53 by Protocol')
plt.xlabel('Protocol')
plt.ylabel('Bytes')
plt.legend(['Uploaded Bytes', 'Downloaded Bytes'])
plt.tight_layout()
plt.savefig('bytes_per_port_53.png')

#############################
## Plot Int vs Ext conns ####
#############################
plt.figure(figsize=(12, 6))
plt.bar(['Internal', 'External'], [Num_internal_conns, Num_external_conns])
plt.title('Number of Internal and External Connections')
plt.ylabel('Number of Connections')
plt.savefig('Internal_vs_External.png')

#############################
## Plot data for IPs ###
#############################
plt.figure(figsize=(12, 6))
bytes_per_ip.plot(kind='bar', stacked=True)
plt.title('Total Bytes Transferred for Ip')
plt.ylabel('Number of Connections')
plt.savefig('bytes_per_ip.png')


#############################
## Country Connections ######
#############################
plt.figure(figsize=(12, 6))
bytes_per_country.plot(kind='bar', x='Country', y='Connections')
plt.title('Number of Connections per Country')
plt.xlabel('Country')
plt.ylabel('Number of Connections')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('bytes_per_country.png')