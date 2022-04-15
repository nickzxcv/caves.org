#!/usr/bin/env python

# written by Will Urbanski ???

import os
import sys
import datetime
import json
import time
import requests
import boto3

CLIENT_ID = 103036

def authenticate(username, password, client_id):
    body = {'ClientID': client_id, 'UserType': 'Admin', 'Username': username, 'password': password}
    r = ym_request("https://ws.yourmembership.com/ams/Authenticate", body)
    return r

def ym_request(api_url, body, debug=False):
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json'}
    r = requests.post(api_url, data=body, headers=headers, timeout=10)
    return r

def ym_get_request(api_url, sessionid, debug=False):
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json', 'x-ss-id': sessionid}
    r = requests.get(api_url, headers=headers, timeout=10)
    return r

def GetMemberIDs(sessionID, clientID):
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept':'application/json', 'x-ss-id': sessionID}

    pageLimit = 1000
    pageNumber = 0

    ids = []

    while True:
        r = requests.get("https://ws.yourmembership.com/Ams/%s/PeopleIDs?UserType=All&PageSize=%d&PageNumber=%d" % (clientID, pageLimit, pageNumber), headers=headers, timeout=10)
        if r.status_code == 200:
            jobj = json.loads(r.text)
            print("Read %d member IDs" % len(jobj['IDList']))
            ids.extend(jobj['IDList'])
            if len(jobj['IDList']) < pageLimit:
                break
            pageNumber += 1
    return ids

def GetMemberProfile(sessionID, clientID, memberID):
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json', 'x-ss-id': sessionID}
    r = requests.get("https://ws.yourmembership.com/Ams/%s/People?ProfileID=%d" % (clientID, memberID),
                     headers=headers, timeout=10)
    if r.status_code == 200:
        jobj = json.loads(r.text)
        return jobj
    return None

def CollectMemberInfo(session_id):
    member_ids = GetMemberIDs(session_id, CLIENT_ID)
    print("Got %d member ids (total)" % len(member_ids))
    local_filename = '/tmp/ymexport.json'
    with open(local_filename, 'w') as f:
        retrieveCount = 0
        for member_id in member_ids:
            memberData = GetMemberProfile(session_id, CLIENT_ID, member_id)
            retrieveCount += 1
            if retrieveCount % 250 == 0:
                print("Processed %d records" % retrieveCount)
                sys.stdout.flush()
            f.write(json.dumps(memberData))
            f.write("\n")

    # upload to members-latest.csv, and time-based path
    # compute current time and time path
    time_now = datetime.datetime.now()
    daily_log = "ym/members/{}.json".format(time_now.strftime("%Y/%m/members-%Y-%m-%d"))

    # build list of paths to write to
    write_paths = ['ym/members-latest.json', daily_log]
    client = boto3.client('s3')
    for keypath in write_paths:
        fptr = open(local_filename, 'rb')
        print("Uploading to ", keypath)
        client.put_object(Bucket="nss-datawarehouse", Key=keypath, Body=fptr)
        fptr.close()

if __name__ == "__main__":

    try:
        user = os.environ['YM_USERNAME']
        password = os.environ['YM_PASSWORD']
    except KeyError:
        print("YM_USERNAME and YM_PASSWORD must be set in the environment")
        sys.exit(1)

    resp = authenticate(user, password, CLIENT_ID)
    if resp.status_code != 200:
        print("Failed to authenticate to YM!")
        print(resp.text)
        sys.exit(1)
    else:
        jobj = json.loads(resp.text)
        session_id = jobj['SessionId']
        print("Session ID: %s" % session_id)

        CollectMemberInfo(session_id)


