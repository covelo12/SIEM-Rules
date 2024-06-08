import pandas as pd
import numpy as np
import pygeoip
import matplotlib.pyplot as plt
import ipaddress

datafile = './../dataset/data6.parquet'  # Data sem anomalias
testfile = './../dataset/test6.parquet'  # Data com anomalias
serverfile = './../dataset/servers6.parquet'  # Requests para ip pubs da intituição

# IP geolocation
gi = pygeoip.GeoIP('./../GeoIP_DBs/GeoIP.dat')
gi2 = pygeoip.GeoIP('./../GeoIP_DBs/GeoIPASNum.dat')

# Read data
data = pd.read_parquet(datafile)
test = pd.read_parquet(testfile)
server = pd.read_parquet(serverfile)

def isInternal(ip):
    if ip.startswith("192"):
        return True
    return False

def CountryFromIP(ip):
    country = gi.country_code_by_addr(ip)
    if not isInternal(ip):
        return country

# Dictionary to count malicious IP occurrences
malicious_ips_count = {}

def update_malicious_ips_count(ips, attack_type):
    for ip in ips:
        if ip in malicious_ips_count:
            if attack_type in malicious_ips_count[ip]:
                malicious_ips_count[ip][attack_type] += 1
            else:
                malicious_ips_count[ip][attack_type] = 1
        else:
            malicious_ips_count[ip] = {attack_type: 1}

########### BotNet activities ###########

def time_btw_requests():
    # Get Time between requests
    data['time_diff'] = data.groupby('src_ip')['timestamp'].diff()
    data['time_diff'] = data['time_diff'] / 100

    # Get the fastest human possible time
    average_time_btw_requests_data = data.groupby('src_ip')['time_diff'].mean().sort_values(ascending=True)[0]
    print("\nAverage Time between requests:", average_time_btw_requests_data)
    
    test['time_diff'] = test.groupby('src_ip')['timestamp'].diff()
    test['time_diff'] = test['time_diff'] / 100
    average_time_btw_requests_test = test.groupby('src_ip')['time_diff'].mean()

    bot_speeds = average_time_btw_requests_test[average_time_btw_requests_test < average_time_btw_requests_data].to_frame(name='bot_speeds')
    
    print("\nIps with bot request speeds:")
    print(bot_speeds.to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(bot_speeds.index, "BotNet")

def internal_conns():
    # Identify internal communications in normal and test datasets
    internal_conns_dst_data = data["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[internal_conns_dst_data & internal_conns_src_data]
    test_internal = test[internal_conns_dst_test & internal_conns_src_test]

    avg_normal_internal = normal_internal.groupby(['src_ip', 'dst_ip']).size().mean()
    normal_internal = normal_internal.groupby(['src_ip', 'dst_ip']).size().to_frame("count")
    test_internal = test_internal.groupby(['src_ip', 'dst_ip']).size().to_frame("count")

    merged_df = pd.merge(test_internal, normal_internal, how='left', on=['src_ip', 'dst_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections:", avg_normal_internal)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 30)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() & (merged_df["count_test"] > avg_normal_internal * 2))]
    print(new_conns.to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index.get_level_values('src_ip'), "BotNet")
    update_malicious_ips_count(new_conns.index.get_level_values('src_ip'), "BotNet")

def internal_ip_conns():
    # Identify internal communications in normal and test datasets
    internal_conns_dst_data = data["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x: isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[internal_conns_dst_data & internal_conns_src_data]
    test_internal = test[internal_conns_dst_test & internal_conns_src_test]

    avg_normal_internal = normal_internal.groupby('src_ip')['dst_ip'].size().mean()
    normal_internal = normal_internal.groupby('src_ip')['dst_ip'].size().to_frame("count")
    test_internal = test_internal.groupby('src_ip')['dst_ip'].size().to_frame("count")

    merged_df = pd.merge(test_internal, normal_internal, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", avg_normal_internal)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 10)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() & (merged_df["count_test"] > avg_normal_internal))]
    print(new_conns.to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index.get_level_values('src_ip'), "BotNet")
    update_malicious_ips_count(new_conns.index.get_level_values('src_ip'), "BotNet")

def countrys_conns():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    avg_country_traffic_data = data.groupby('country').size().mean()
    country_traffic_data = data.groupby('country').size().to_frame("count")
    country_traffic_test = test.groupby('country').size().to_frame("count")

    merged_df = pd.merge(country_traffic_test, country_traffic_data, how='left', on=['country'], suffixes=('_test', '_data'))

    print("Average Connections per country: ", avg_country_traffic_data)
    print("\nExisting Connections: ")
    print(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 2)].to_string())
    print("\nNew Connections: ")
    print(merged_df.loc[merged_df["count_data"].isna() & (merged_df["count_test"] > avg_country_traffic_data)].to_string())

########### Exfiltration ###########

def internal_external_conns():
    # Identify internal communications in normal and test datasets
    internal_conns_dst_data = data["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[internal_conns_dst_data & internal_conns_src_data]
    test_internal = test[internal_conns_dst_test & internal_conns_src_test]

    avg_normal_internal = normal_internal.groupby(['src_ip', 'dst_ip']).size().mean()
    normal_internal = normal_internal.groupby(['src_ip', 'dst_ip']).size().to_frame("count")
    test_internal = test_internal.groupby(['src_ip', 'dst_ip']).size().to_frame("count")

    merged_df = pd.merge(test_internal, normal_internal, how='left', on=['src_ip', 'dst_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections:", avg_normal_internal)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 100)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() & (merged_df["count_test"] > avg_normal_internal * 10))]
    print(new_conns.to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index.get_level_values('src_ip'), "Exfiltration")
    update_malicious_ips_count(new_conns.index.get_level_values('src_ip'), "Exfiltration")

def internal_external_ip_conns():
    # Identify internal communications in normal and test datasets
    internal_conns_dst_data = data["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_data = data['src_ip'].apply(lambda x: isInternal(x))
    internal_conns_dst_test = test["dst_ip"].apply(lambda x: not isInternal(x))
    internal_conns_src_test = test['src_ip'].apply(lambda x: isInternal(x))

    normal_internal = data[internal_conns_dst_data & internal_conns_src_data]
    test_internal = test[internal_conns_dst_test & internal_conns_src_test]

    avg_normal_internal = normal_internal.groupby('src_ip')['dst_ip'].size().mean()
    normal_internal = normal_internal.groupby('src_ip')['dst_ip'].size().to_frame("count")
    test_internal = test_internal.groupby('src_ip')['dst_ip'].size().to_frame("count")

    merged_df = pd.merge(test_internal, normal_internal, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", avg_normal_internal)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 50)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() & (merged_df["count_test"] > avg_normal_internal * 10))]
    print(new_conns.to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index.get_level_values('src_ip'), "Exfiltration")
    update_malicious_ips_count(new_conns.index.get_level_values('src_ip'), "Exfiltration")

def tcp_exfiltration():
    tcp_data_mean = data[data['proto'] == "tcp"]['up_bytes'].mean() * 2

    avg_data_tcp = data[(data['proto'] == "tcp") & (data['up_bytes'] > tcp_data_mean)].groupby('src_ip')['up_bytes'].size().mean()
    data_tcp = data[(data['proto'] == "tcp") & (data['up_bytes'] > tcp_data_mean)].groupby('src_ip')['up_bytes'].size().to_frame("count")
    test_tcp = test[(test['proto'] == "tcp") & (test['up_bytes'] > tcp_data_mean)].groupby('src_ip')['up_bytes'].size().to_frame("count")
    
    merged_df = pd.merge(test_tcp, data_tcp, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", avg_data_tcp)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 20)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > avg_data_tcp))]
    print(new_conns.to_string())

    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index, "Exfiltration")
    update_malicious_ips_count(new_conns.index, "Exfiltration")

def tcp_exfiltration_per_ip():
    tcp_data_mean = data[data['proto'] == "tcp"].groupby('src_ip')['up_bytes'].sum().mean()
    data_tcp = data[data['proto'] == "tcp"].groupby('src_ip')['up_bytes'].sum().to_frame("count")
    test_tcp = test[test['proto'] == "tcp"].groupby('src_ip')['up_bytes'].sum().to_frame("count")
    
    merged_df = pd.merge(test_tcp, data_tcp, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", tcp_data_mean)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 20)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > tcp_data_mean))]
    print(new_conns.to_string())

    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index, "Exfiltration")
    update_malicious_ips_count(new_conns.index, "Exfiltration")

def udp_exfiltration():
    udp_data_mean = data[data['proto'] == "udp"]['up_bytes'].mean()

    avg_data_udp = data[(data['proto'] == "udp") & (data['up_bytes'] > udp_data_mean) ].groupby('src_ip')['up_bytes'].size().mean()
    data_udp = data[(data['proto'] == "udp") & (data['up_bytes'] > udp_data_mean) ].groupby('src_ip')['up_bytes'].size().to_frame("count")
    test_udp = test[(test['proto'] == "udp") & (test['up_bytes'] > udp_data_mean) ].groupby('src_ip')['up_bytes'].size().to_frame("count")
    
    merged_df = pd.merge(test_udp, data_udp, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", avg_data_udp)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 20)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > avg_data_udp))]
    print(new_conns.to_string())

    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index, "Exfiltration")
    update_malicious_ips_count(new_conns.index, "Exfiltration")

def udp_exfiltration_per_ip():
    udp_data_mean = data[data['proto'] == "udp"].groupby('src_ip')['up_bytes'].sum().mean()
    data_udp = data[data['proto'] == "udp"].groupby('src_ip')['up_bytes'].sum().to_frame("count")
    test_udp = test[test['proto'] == "udp"].groupby('src_ip')['up_bytes'].sum().to_frame("count")
    
    merged_df = pd.merge(test_udp, data_udp, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", udp_data_mean)
    print("\nExisting Connections: ")
    existing_conns = merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 20)]
    print(existing_conns.to_string())
    print("\nNew Connections: ")
    new_conns = merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > udp_data_mean))]
    print(new_conns.to_string())

    # Update malicious IPs count
    update_malicious_ips_count(existing_conns.index, "Exfiltration")
    update_malicious_ips_count(new_conns.index, "Exfiltration")

def countrys_up():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    avg_country_traffic_data = data.groupby('country')["up_bytes"].sum().mean()
    country_traffic_data = data.groupby('country')["up_bytes"].sum().to_frame("count")
    country_traffic_test = test.groupby('country')["up_bytes"].sum().to_frame("count")

    merged_df = pd.merge(country_traffic_test, country_traffic_data, how='left', on=['country'], suffixes=('_test', '_data'))

    print("Average Uploads per country: ", avg_country_traffic_data)
    print("\nExisting Connections: ")
    print(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 2)].to_string())
    print("\nNew Connections: ")
    print(merged_df.loc[merged_df["count_data"].isna() & (merged_df["count_test"] > avg_country_traffic_data)].to_string())

########### C&C ###########
def udp_cc():

    udp_data_mean = data[data['proto'] == "udp"]['down_bytes'].mean() * 2

    avg_data_udp = data[(data['proto'] == "udp") & (data['down_bytes'] > udp_data_mean) ].groupby('src_ip')['down_bytes'].size().mean()
    data_udp = data[(data['proto'] == "udp") & (data['down_bytes'] > udp_data_mean) ].groupby('src_ip')['down_bytes'].size().to_frame("count")
    test_udp = test[(test['proto'] == "udp") & (test['down_bytes'] > udp_data_mean) ].groupby('src_ip')['down_bytes'].size().to_frame("count")
    
    merged_df = pd.merge(test_udp, data_udp, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", avg_data_udp)
    print("\nExisting Connections: ")
    print(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 10)].to_string())
    print("\nNew Connections: ")
    print(merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > avg_data_udp * 10))].to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 10)].index, "C&C")
    update_malicious_ips_count(merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > avg_data_udp * 10))].index, "C&C")

def udp_cc_per_ip():

    udp_data_mean = data[data['proto'] == "udp"].groupby('src_ip')['down_bytes'].sum().mean()
    data_udp = data[data['proto'] == "udp"].groupby('src_ip')['down_bytes'].sum().to_frame("count")
    test_udp = test[test['proto'] == "udp"].groupby('src_ip')['down_bytes'].sum().to_frame("count")
    
    merged_df = pd.merge(test_udp, data_udp, how='left', on=['src_ip'], suffixes=('_test', '_data'))
    
    print("Average Connections per src ip: ", udp_data_mean)
    print("\nExisting Connections: ")
    print(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 10)].to_string())
    print("\nNew Connections: ")
    print(merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > udp_data_mean))].to_string())
    
    # Update malicious IPs count
    update_malicious_ips_count(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 10)].index, "C&C")
    update_malicious_ips_count(merged_df.loc[(merged_df["count_data"].isna() &  (merged_df["count_test"] > udp_data_mean))].index, "C&C")

def countrys_down():

    # Extract country data for both data and test
    data['country'] = data['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')
    test['country'] = test['dst_ip'].drop_duplicates().apply(lambda y: CountryFromIP(y)).to_frame(name='cc')

    # Group by country and sum the down_bytes for each country
    avg_country_traffic_data = data.groupby('country')["down_bytes"].sum().mean()
    country_traffic_data = data.groupby('country')["down_bytes"].sum().to_frame("count")
    country_traffic_test = test.groupby('country')["down_bytes"].sum().to_frame("count")

    merged_df = pd.merge(country_traffic_test, country_traffic_data, how='left', on=['country'], suffixes=('_test', '_data'))

    print("Average Downloads per country: ", avg_country_traffic_data)
    print("\nExisting Connections: ")
    print(merged_df.loc[(merged_df["count_test"] > merged_df["count_data"] * 2)].to_string())
    print("\nNew Connections: ")
    print(merged_df.loc[merged_df["count_data"].isna() & (merged_df["count_test"] > avg_country_traffic_data)].to_string())

time_btw_requests()
internal_conns()
internal_ip_conns()
internal_external_conns()
internal_external_ip_conns()
tcp_exfiltration()
tcp_exfiltration_per_ip()
udp_exfiltration()
udp_exfiltration_per_ip()
udp_cc()
udp_cc_per_ip()

countrys_conns()
countrys_down()
countrys_up()

print("\nMalicious IPs:")
sorted_ips = sorted(malicious_ips_count.items(), key=lambda x: sum(x[1].values()), reverse=True)
for ip, counts in sorted_ips:
    total_count = sum(counts.values())
    print(f"IP: {ip}, Total Count: {total_count}")
    for attack_type, count in counts.items():
        print(f"  - {attack_type}: {count}")


