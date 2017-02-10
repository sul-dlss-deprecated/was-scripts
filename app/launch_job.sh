#!/bin/bash

JOB_ID=$1
# Run the job
TEMP_RES=`curl -sS  -d "action=launch" -k -u <uname:pwd> --anyauth --location -H "Accept: application/xml" https://<uri>:<port>/<path>/$1`

# Wait to avoid the preparation period
sleep 10

# Check status
RESPONSE=`curl -sS  -k -u <uname:pwd> --anyauth --location -H "Accept: application/xml" https://<uri>:<port>/<path>/$1`

if [[ "$RESPONSE" =~ "<crawlControllerState>PAUSED</crawlControllerState>" ]]; then
  TEMP_RES=`curl -sS  -d "action=unpause" -k -u <uname:pwd> --anyauth --location -H "Accept: application/xml" https://<uri>:<port>/<path>/$1`
  sleep 5
  RESPONSE=`curl -sS  -k -u <uname:pwd> --anyauth --location -H "Accept: application/xml" https://<uri>:<port>/<path>/$1`
fi

if [[ "$RESPONSE" =~ "<crawlControllerState>RUNNING</crawlControllerState>" ]]; then
  exit
else
 echo $RESPONSE 1>&2
 exit 80;
fi
