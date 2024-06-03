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


