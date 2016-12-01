# Script will convert existing log files from Splunk to GrayLog

# Imports
import os
import time
import splunklib.results as results
import splunklib.client as client
import requests as requests

# Base configuration
# Splunk search info
HOST = "Splunk"
PORT = 8089
USERNAME = "admin"
PASSWORD = "redacted"
lineCount = 10
searchModifier = "Host=DFSBack"
# Graylog server info
graylogPath = "http://graylog:12201/"
# Program configuration
if os.name == 'nt':
    path = "C:\\ProgramData\\SplunkConv\\"
else:
    path = '/var/spool/SplunkConv/'
# logs = path+'logs.csv'
timeMark = path+'time'

# Check for path and then files
if not os.path.isfile(timeMark):
    endTime = time.time()
    if not os.path.isdir(path):
        os.mkdir(path)
else:
    with open(timeMark, 'r') as timeFile:
        endTime = timeFile.read()

# Query Splunk and get results in JSON
service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
kwargs = {"endtime": endTime, "timeformat": "%s", "count": lineCount, "output_mode": "json"}
searchQuery = "search "+searchModifier
oneshot = service.jobs.oneshot(searchQuery, **kwargs)
reader = results.ResultsReader(oneshot)

# Fill each record to a GELF log
for res in reader:
    gelf = {}
    gelf["version"] = "1.1"
    gelf["host"] = res["host"]
    gelf["short_message"] = res["_raw"]
    gelf["timestamp"] = res["_indextime"]
    endTime = res["_indextime"]
    requests.post(graylogPath, gelf)

with open(timeMark, 'w') as timeFile:
    timeFile.write(endTime)