#!/usr/bin/env python
import requests
import random
import sys
import xml.etree.ElementTree as ET
import time
import csv
import base64
import datetime
import hashlib
import os
import boto3

api_url = "https://api.yourmembership.com"

def ym_request(body, debug=False):
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
r = requests.post(api_url, data=request_body, headers=headers, timeout=10)
if debug == True: print(r.text)

# create element tree object
tree = ET.fromstring(r.text)
item = tree.find("ErrCode")
if item.text == "0":
# success
return tree
else:
return False

def is_active_member(member):
#print(member)
if member['Approved_Site_Member'] == 'Yes':
if 'Life' in member['Membership']:
return True
else:
if member['Date_Membership_Expires'] != '':
expiration_ts = time.mktime(datetime.datetime.strptime(member['Date_Membership_Expires'], "%Y-%m-%d %H:%M:%S").timetuple())
if expiration_ts > time.time():
return True
print("%s => %s => expired" % (member['Constituent_ID'], member['Date_Membership_Expires']))
return False

ym_private_api_key = os.environ['YM_PRIVATE_API_KEY']
ym_public_api_key = os.environ['YM_PUBLIC_API_KEY']
ym_sa_passcode = os.environ['YM_SA_PASSCODE']

print("Requesting member export...")
request_body = """<?xml version="1.0" encoding="utf-8" ?>
<YourMembership>
    <Version>2.25</Version>
    <ApiKey>%s</ApiKey>
    <CallID>%s</CallID>
    <SaPasscode>%s</SaPasscode>
    <Call Method="Sa.Export.Members">
        <Unicode>0</Unicode>
        <CustomFields>1</CustomFields>
    </Call>
</YourMembership>""" % (ym_private_api_key, random.randint(10000,999999999), ym_sa_passcode)
r = ym_request(request_body)
if r != False:
item = r.find("Sa.Export.Members/ExportID")
export_id = item.text
print("ExportId is {}".format(export_id))
else:
sys.exit(1)

time.sleep(5)

blocked = True
while blocked == True:
# poll for the status to change
# https://api.yourmembership.com/reference/2_25/Sa_Export_Status.htm
request_body = """<?xml version="1.0" encoding="utf-8" ?>

<YourMembership>
   <Version>2.25</Version>
   <ApiKey>%s</ApiKey>
   <CallID>001</CallID>
   <SaPasscode>%s</SaPasscode>
   <Call Method="Sa.Export.Status">
       <ExportID>%s</ExportID>
   </Call>
</YourMembership>""" %(ym_private_api_key, ym_sa_passcode, export_id)
print("Requesting update on export status...")
r = ym_request(request_body)
if r != False:
item = r.find("Sa.Export.Status/Status")
status = item.text
if status != "2":
print("Status is %s, sleeping." % status)
time.sleep(5)
else:
blocked = False
print("Status is done!")
item = r.find("Sa.Export.Status/ExportURI")
url = item.text

# request the file
print("Downloading export")
local_filename = 'members.csv'
r = requests.get(url, stream=True)
with open(local_filename, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024): 
        if chunk: # filter out keep-alive new chunks
            f.write(chunk)
            #f.flush() commented by recommendation from J.F.Sebastian

# upload to members-latest.csv, and time-based path

# compute current time and time path
time_now = datetime.datetime.now()
daily_log = "ym/members/{}.csv".format(time_now.strftime("%Y/%m/members-%Y-%m-%d"))

#build list of paths to write to
write_paths = ['ym/members-latest.csv', daily_log]
client = boto3.client('s3')
for keypath in write_paths:
fptr = open(local_filename,'rb')
print("Uploading to ", keypath)
client.put_object(Bucket="nss-datawarehouse", Key=keypath, Body=fptr)
fptr.close()

