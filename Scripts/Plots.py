import pandas as pd
import numpy as np
import pygeoip
import matplotlib.pyplot as plt 
import ipaddress


datafile = './../dataset/data6.parquet' #Data sem anomalias
testfile = './../dataset/test6.parquet' #Data com anomalias
serverfile = './../dataset/servers6.parquet' # Requests para ip pubs da intituição


# IP geolocation
gi = pygeoip.GeoIP('./../GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./../GeoIP_DBs/GeoIPASNum.dat')

# Read data
data = pd.read_parquet(datafile)
test = pd.read_parquet(testfile)
server = pd.read_parquet(serverfile)

def isInternal(ip):
    if(ip.startswith("192")):
        return True
    return False 

def CountryFromIP(ip):
    return gi.country_code_by_addr(ip)


###################################
######## PORT VS BYTES ############
###################################
def data_per_port(data=data, test=test):
    bytes_per_porto_data = data.groupby("proto")[['up_bytes', 'down_bytes']].sum()
    bytes_per_porto_test = test.groupby("proto")[['up_bytes', 'down_bytes']].sum()

    print("Data\n", bytes_per_porto_data)
    print("Test\n", bytes_per_porto_test)
#data_per_port()

#############################
## Plot Int vs Ext conns ####
#############################
def data_int_ext(data=data, test=test):

    Num_internal_conns = data["dst_ip"].apply(lambda x:isInternal(x)).sum()
    Num_external_conns = data['dst_ip'].apply(lambda x: not isInternal(x)).sum()

    print("Data")
    print("External Connections", Num_external_conns)
    print("Internal Connections", Num_internal_conns)

    Num_internal_conns = test["dst_ip"].apply(lambda x:isInternal(x)).sum()
    Num_external_conns = test['dst_ip'].apply(lambda x: not isInternal(x)).sum()

    print("Test")
    print("External Connections", Num_external_conns)
    print("Internal Connections", Num_internal_conns)

    plt.figure(figsize=(12, 6))
    plt.bar(['Internal', 'External'], [Num_internal_conns, Num_external_conns])
    plt.title('Number of Internal and External Connections')
    plt.ylabel('Number of Connections')
    plt.savefig('img/Internal_vs_External.png')
#data_int_ext()

#############################
## Plot Int vs Ext IP conns ####
#############################
def data_from_int_conn(data=data, test=test):

    internal_conns_dst_data = data["dst_ip"].apply(lambda x:isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x:isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x:isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x:isInternal(x))

    ip_num_conns_data = data[internal_conns_dst_data][internal_conns_src_data].groupby("dst_ip")["src_ip"].size()
    ip_num_conns_test = test[internal_conns_dst_test][internal_conns_src_test].groupby("dst_ip")["src_ip"].size()

    merged_data = pd.merge(ip_num_conns_data, ip_num_conns_test, on='dst_ip', suffixes=('_data', '_test'),  how='outer').nlargest(20, "src_ip_test")
    merged_data = merged_data.fillna(0)

    print(merged_data)
#data_from_int_conn()

#############################
## Plot Int vs Ext IP conns ####
#############################
def data_to_int_conn(data=data, test=test):

    internal_conns_dst_data = data["dst_ip"].apply(lambda x:isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x:isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x:isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x:isInternal(x))

    ip_num_conns_data = data[internal_conns_dst_data][internal_conns_src_data].groupby("src_ip")["dst_ip"].size()
    ip_num_conns_test = test[internal_conns_dst_test][internal_conns_src_test].groupby("src_ip")["dst_ip"].size()

    merged_data = pd.merge(ip_num_conns_data, ip_num_conns_test, on='src_ip', suffixes=('_data', '_test'),  how='outer').nlargest(20, "dst_ip_test")
    merged_data = merged_data.fillna(0)

    print(merged_data)

#data_to_int_conn()


#############################
## Plot Int vs Ext IP conns ####
#############################
def data_to_external_conn(data=data, test=test):

    external_conns_dst_data = data["dst_ip"].apply(lambda x:not isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x:isInternal(x))
    external_conns_dst_test = test["dst_ip"].apply(lambda x:not isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x:isInternal(x))

    ip_num_conns_data = data[external_conns_dst_data][internal_conns_src_data].groupby("src_ip")["dst_ip"].size()
    ip_num_conns_test = test[external_conns_dst_test][internal_conns_src_test].groupby("src_ip")["dst_ip"].size()

    merged_data = pd.merge(ip_num_conns_data, ip_num_conns_test, on='src_ip', suffixes=('_data', '_test'),  how='outer').nlargest(20, "dst_ip_test")
    merged_data = merged_data.fillna(0)

    print(merged_data)

#data_to_external_conn()

############################
##### TimeFrame ############
############################
def timeFrame(data=data,test=test):

    # Create a new column to store the time difference between requests for each IP
    data['time_diff'] = data.groupby('src_ip')['timestamp'].diff()
    # Convert timestamp differences from units of 1/100th of a second to seconds
    data['time_diff'] = data['time_diff'] / 100
    # Calculate the average time difference for each IP
    average_time_btw_requests_data = data.groupby('src_ip')['time_diff'].mean()

    # Create a new column to store the time difference between requests for each IP
    test['time_diff'] = test.groupby('src_ip')['timestamp'].diff()
    # Convert timestamp differences from units of 1/100th of a second to seconds
    test['time_diff'] = test['time_diff'] / 100
    # Calculate the average time difference for each IP
    average_time_btw_requests_test = test.groupby('src_ip')['time_diff'].mean()
    # Merge the two dataframes on country code
    merged_data = pd.merge(average_time_btw_requests_data, average_time_btw_requests_test, on='src_ip', suffixes=('_data', '_test'),  how='outer').nsmallest(10, "time_diff_test")
    merged_data = merged_data.fillna(0)

    plt.figure(figsize=(12, 6))
    merged_data.plot(kind='bar', stacked=True)
    plt.title('Diferences between datapackets')
    plt.xlabel('ip')
    plt.ylabel('Time diference')
    plt.legend(['Clean dataset', 'Anomalous dataset'])
    plt.tight_layout()
    plt.savefig('img/timeFrame.png')
#timeFrame()

##############################
###### Country VS Bytes ######
##############################
def compCountryBytes(data=data,test=test):   

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
#compCountryBytes()

#################################
########### IP VS BYTES #########
#################################
def compIpBytes(data=data,test=test):
    up_data = data.groupby('src_ip')['up_bytes'].sum()
    up_test = test.groupby('src_ip')['up_bytes'].sum()

    # Merge the two dataframes on country code
    merged_data = pd.merge(up_data, up_test, on='src_ip', suffixes=('_data', '_test'),  how='outer').nlargest(10, "up_bytes_test")
    merged_data = merged_data.fillna(0)

    # Convert the columns back to integer type
    merged_data['up_bytes_data'] = merged_data['up_bytes_data'].astype(int)
    merged_data['up_bytes_test'] = merged_data['up_bytes_test'].astype(int)

    # Plotting
    plt.figure(figsize=(12, 6))
    merged_data.plot(kind='bar', stacked=True)
    plt.title('Diferences between datapackets')
    plt.xlabel('ip')
    plt.ylabel('Upload bytes')
    plt.legend(['Clean dataset', 'Anomalous dataset'])
    plt.tight_layout()
    plt.savefig('img/up_bytes_ip.png')

    down_data = data.groupby('src_ip')['down_bytes'].sum()
    down_test = test.groupby('src_ip')['down_bytes'].sum()

    # Merge the two dataframes on country code
    merged_data = pd.merge(down_data, down_test, on='src_ip', suffixes=('_data', '_test'),  how='outer').nlargest(10, "down_bytes_test")
    merged_data = merged_data.fillna(0)

    # Convert the columns back to integer type
    merged_data['down_bytes_data'] = merged_data['down_bytes_data'].astype(int)
    merged_data['down_bytes_test'] = merged_data['down_bytes_test'].astype(int)

    # Plotting
    plt.figure(figsize=(12, 6))
    merged_data.plot(kind='bar', stacked=True)
    plt.title('Diferences between datapackets')
    plt.xlabel('ip')
    plt.ylabel('Download bytes')
    plt.legend(['Clean dataset', 'Anomalous dataset'])
    plt.tight_layout()
    plt.savefig('img/down_bytes_ip.png')
#compIpBytes()


def internal_services(data=data, test=test):

    internal_conns_dst_data = data["dst_ip"].apply(lambda x: isInternal(x))
    normal_internal = data[internal_conns_dst_data].groupby('dst_ip').size().to_frame("count")
    print(normal_internal.nlargest(10, "count"))


