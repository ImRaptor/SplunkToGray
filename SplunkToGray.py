# Script will convert existing log files from Splunk to GrayLog

# Imports
import os
import time
import yaml
import splunklib.results as results
import splunklib.client as client
import requests as requests

# Base configuration
# Splunk search info
HOST = "Splunk"  # Splunk server
PORT = 8089  # Splunk REST API port
USERNAME = "admin"
PASSWORD = "redacted"
lineCount = 1000  # Number of results to process at a time
searchModifier = "Host=DFSBack"  # Search modifier to target a subset of data. Set to * for everything

#  Graylog server info
graylogPath = "http://graylog:12202/gelf"  # Address of a configured GELF HTTP Input on a Graylog server

# Program configuration
if os.name == 'nt':
    path = "C:\\ProgramData\\SplunkConv\\"
else:
    path = '/var/spool/SplunkConv/'
timeMark = path+'time'

# Check for file and path
endTime = ''
if not os.path.isdir(path):
    os.mkdir(path)
elif os.path.isfile(timeMark):
    with open(timeMark, 'r') as timeFile:
        readBuff = yaml.load(timeFile)
        for line in readBuff['search']:
            if line['modifier'] == searchModifier:
                endTime = line['time']

# Deal with blank time
if endTime is '':
    endTime = str(int(time.time()))

# Query Splunk and get results in JSON
service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
kwargs = {"latest_time": endTime, "time_format": "%s", "count": lineCount, "output_mode": "json"}
searchQuery = "search "+searchModifier+"|head "+str(lineCount)
oneshot = service.jobs.oneshot(searchQuery, **kwargs)
reader = results.ResultsReader(oneshot)

recTime = 0
# Fill each record to a GELF log
for res in reader:
    gelf = {"version": "1.1",
            "host": res["host"],
            "short_message": res["_raw"],
            "timestamp": res["_indextime"]}
    recTime = int(res["_indextime"])
    requests.post(graylogPath, json=gelf)

with open(timeMark, mode='rw') as timeFile:
    buff = yaml.load(timeFile)
    stack = {'search': []}
    for line in buff['search']:
        if line['modifier'] == searchModifier:
            line['time'] = str(recTime-1)  # Move end time 1 second back to prevent retrieving the same logs again
        stack['search'].append(line)
    timeFile.write(yaml.dump(stack))
