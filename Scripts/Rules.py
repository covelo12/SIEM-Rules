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
    
    avg_internal_test = normal_internal.groupby(['src_ip', 'dst_ip']).size()

    # ver quanto a mais da trgger
    print(avg_internal_test[avg_internal_test > avg_internal_data * 3])

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

    # ver quanto a mais da trgger
    print(avg_internal_test[avg_internal_test > avg_internal_data * 2])


def countrys_conns():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: gi.country_code_by_addr(y)).to_frame(name='cc')
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: gi.country_code_by_addr(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    country_traffic_data = data.groupby('country').size().mean()
    print(country_traffic_data)
    country_traffic_test = test.groupby('country').size()

    # Existing countrys need to go up 3 times
    print(country_traffic_test[country_traffic_test > country_traffic_data * 3])

    countries_not_in_data = test[~test['country'].isin(data['country'])]['country'].unique()

    # Non Existing need to be above average
    print(country_traffic_test[countries_not_in_data][country_traffic_test > country_traffic_data])


def tcp_exfiltration():
    # Filter data and test datasets to include only rows where the protocol is "tcp"
    tcp_data_mean = data[data['proto'] == "tcp"]['up_bytes'].mean()
    tcp_test_above_mean = test[(test['proto'] == "tcp") & (test['up_bytes'] > tcp_data_mean * 10)]

    # Get the source IPs for each request in the test dataset where the up bytes exceed ten times the mean in the data dataset
    source_ips = tcp_test_above_mean['src_ip'].unique()

    # Print the mean up bytes for the TCP protocol in the data dataset
    print("Mean up bytes for tcp in data:", tcp_data_mean)

    # Print the source IPs for each request in the test dataset that exceeds ten times the mean in the data dataset
    for ip in source_ips:
        print("Source IP:", ip)

# Call the function
tcp_exfiltration()
