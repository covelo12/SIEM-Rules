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



# Bot net Activity
def time_btw_requests():

    data['time_diff'] = data.groupby('src_ip')['timestamp'].diff()
    data['time_diff'] = data['time_diff'] / 100
    average_time_btw_requests_data = data.groupby('src_ip')['time_diff'].mean().sort_values(ascending=True)[0]
    print("average request time:", average_time_btw_requests_data)
    test['time_diff'] = test.groupby('src_ip')['timestamp'].diff()
    test['time_diff'] = test['time_diff'] / 100
    ip_speed= test.groupby('src_ip')['time_diff'].mean()

    print(ip_speed[ip_speed < average_time_btw_requests_data])


def internal_conns():
    # Identify internal communications in normal and test datasets
    internal_conns_dst_data = data["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[internal_conns_dst_data & internal_conns_src_data]
    test_internal = test[internal_conns_dst_test & internal_conns_src_test]

    avg_internal_data = normal_internal.groupby(['src_ip', 'dst_ip']).size().mean()
    
    avg_internal_test = test_internal.groupby(['src_ip', 'dst_ip']).size()

    print("Average:", avg_internal_data)
    print(avg_internal_test[avg_internal_test > avg_internal_data * 2])

def internal_ip_conns():
    # Identify internal communications in normal and test datasets
    internal_conns_dst_data = data["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[internal_conns_dst_data & internal_conns_src_data]
    test_internal = test[internal_conns_dst_test & internal_conns_src_test]

    avg_internal_data = normal_internal.groupby(['src_ip'])['dst_ip'].size().mean()
    avg_internal_test = test_internal.groupby(['src_ip'])['dst_ip'].size()

    print("Average:", avg_internal_data)
    print(avg_internal_test[avg_internal_test > avg_internal_data * 2])

def internal_external_conns():
    # Identify internal communications in normal and test datasets
    external_conns_dst_data = data["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    external_conns_dst_test = test["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[external_conns_dst_data & internal_conns_src_data]
    test_internal = test[external_conns_dst_test & internal_conns_src_test]

    avg_internal_data = normal_internal.groupby(['src_ip', 'dst_ip']).size().mean()
    avg_internal_test = test_internal.groupby(['src_ip', 'dst_ip']).size()

    print("Average:", avg_internal_data)
    # N seii se é demais
    print(avg_internal_test[avg_internal_test > avg_internal_data * 20 ])

def internal_external_ip_conns():
    # Identify internal communications in normal and test datasets
    external_conns_dst_data = data["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    external_conns_dst_test = test["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[external_conns_dst_data & internal_conns_src_data]
    test_internal = test[external_conns_dst_test & internal_conns_src_test]

    avg_internal_data = normal_internal.groupby(['src_ip'])['dst_ip'].size().mean()
    avg_internal_test = test_internal.groupby(['src_ip'])['dst_ip'].size()

    print("Average:", avg_internal_data)
    print(avg_internal_test[avg_internal_test > avg_internal_data * 2 ])


def countrys_conns():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')
    print(data["country"].unique())
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    country_traffic_data = data.groupby('country').size()
    country_traffic_test = test.groupby('country').size()

    # Reindex the data series to ensure they have the same indices
    country_traffic_test_aligned = country_traffic_test.reindex(country_traffic_data.index, fill_value=0)

    # Identify countries where the number of connections in the test set is more than twice the number in the original dataset
    print(country_traffic_test_aligned[country_traffic_test_aligned > country_traffic_data * 2])

    countries_not_in_data = test[~test['country'].isin(data['country'])]['country'].unique()

    country_traffic_data = data.groupby('country').size().mean()
    print(country_traffic_data)
    # Non Existing need to be above average
    print(country_traffic_test[countries_not_in_data][country_traffic_test > country_traffic_data])
countrys_conns()
def countrys_up():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    country_traffic_data = data.groupby('country')["up_bytes"].sum()
    country_traffic_test = test.groupby('country')["up_bytes"].sum()

    # Reindex the data series to ensure they have the same indices
    country_traffic_test_aligned = country_traffic_test.reindex(country_traffic_data.index, fill_value=0)

    # Identify countries where the number of connections in the test set is more than twice the number in the original dataset
    print(country_traffic_test_aligned[country_traffic_test_aligned > country_traffic_data * 2])

    countries_not_in_data = test[~test['country'].isin(data['country'])]['country'].unique()

    country_traffic_data = data.groupby('country')["up_bytes"].sum().mean()
    print(country_traffic_data)
    # Non Existing need to be above average
    print(country_traffic_test[countries_not_in_data][country_traffic_test > country_traffic_data])

def countrys_down():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    country_traffic_data = data.groupby('country')["down_bytes"].sum()
    country_traffic_test = test.groupby('country')["down_bytes"].sum()

    # Reindex the data series to ensure they have the same indices
    country_traffic_test_aligned = country_traffic_test.reindex(country_traffic_data.index, fill_value=0)

    # Identify countries where the number of connections in the test set is more than twice the number in the original dataset
    print(country_traffic_test_aligned[country_traffic_test_aligned > country_traffic_data * 2])

    countries_not_in_data = test[~test['country'].isin(data['country'])]['country'].unique()

    country_traffic_data = data.groupby('country')["down_bytes"].sum().mean()
    print(country_traffic_data)
    # Non Existing need to be above average
    print(country_traffic_test[countries_not_in_data][country_traffic_test > country_traffic_data])

# Exfiltration

def tcp_exfiltration():

    tcp_data_mean = data[data['proto'] == "tcp"]['up_bytes'].mean()
    
    tcp_test_above_mean = test[(test['proto'] == "tcp") & (test['up_bytes'] > tcp_data_mean * 20)]
    # Group by source IP and sum up the bytes
    tcp_test_grouped = tcp_test_above_mean.groupby('src_ip')['up_bytes'].size()

    # Print the mean up bytes for the TCP protocol in the data dataset
    print("Mean up bytes for tcp in data:", tcp_data_mean)
    print(tcp_test_grouped)


def tcp_exfiltration_per_ip():
    
    tcp_data_mean = data[data['proto'] == "tcp"].groupby('src_ip')['up_bytes'].sum().mean()
    tcp_test_above_mean = test[(test['proto'] == "tcp") & (test['up_bytes'] > tcp_data_mean)].groupby('src_ip')['up_bytes'].sum()

    print(tcp_test_above_mean)


## C&C
def udp_cc():

    udp_data_mean = data[data['proto'] == "udp"]['down_bytes'].mean()
    
    udp_test_above_mean = test[(test['proto'] == "udp") & (test['down_bytes'] > udp_data_mean * 2)]
    # Group by source IP and sum up the bytes
    udp_test_grouped = udp_test_above_mean.groupby('src_ip')['down_bytes'].size()

    # Print the mean up bytes for the udp protocol in the data dataset
    print("Mean up bytes for udp in data:", udp_data_mean)
    print(udp_test_grouped)


def udp_cc_per_ip():
    
    udp_data_mean = data[data['proto'] == "udp"].groupby('src_ip')['down_bytes'].sum().mean()
    udp_test_above_mean = test[(test['proto'] == "udp") & (test['down_bytes'] > udp_data_mean)].groupby('src_ip')['down_bytes'].sum()

    print(udp_test_above_mean)
