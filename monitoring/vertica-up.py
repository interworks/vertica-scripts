#!/usr/bin/python

from subprocess import *
from email.mime.text import MIMEText
import pickle
from os import path
from datetime import datetime
from datetime import timedelta

#######################################
## BEGIN configurable options
#######################################
vsql = '/opt/vertica/bin/vsql'
verticaHost = '192.168.1.1'
verticaUser = 'dbuser'
verticaPassword = 'dbpassword'

stateFile = '.verticaNodeMonitor'
minutesBeforeRepeat = 1
mailFrom = "no-reply@foo.com"
mailTo = "your-email@foo.com"

#######################################
## END configurable options
#######################################

mailText = ""
mailSubject = ""
state = 'UP'
lastAlertStatus = { 'state': 'UP', 'lastTime': False }

if path.isfile(stateFile):
    print "Loading status"
    f = open(stateFile, 'r')
    lastAlertStatus = pickle.load(f)
    f.close()

try:
    qs = "SELECT CONCAT(CONCAT(node_name, ': '), node_state) FROM nodes WHERE node_state != 'UP'"
    args = [vsql, '-Atq', '-h', verticaHost, '-U', verticaUser, '-w', verticaPassword, '-c', qs]
    proc = Popen(args, stdout=PIPE)
    results = proc.communicate()[0].splitlines()
    ret = proc.returncode

    if ret != 0:
        raise Exception("vsql returned error")

    if len(results) > 0:
        state = 'DOWN'
        mailText = "\n".join(results)
        mailSubject = "Vertica Alert: %d node(s) in non-UP state" % len(results)

except:
        mailSubject = "Vertica Alert: Error while testing node state"
        mailText = "An unknown error occurred while attempting to determine Vertica node states (entire cluster may be down)"
        state = 'ERROR'

if state == 'UP':
    mailSubject = "Vertica Alert: All nodes are UP"
    mailText = "All nodes are up"

repeatTime = datetime.now() - timedelta(minutes=minutesBeforeRepeat)

if lastAlertStatus['state'] != state or (state != 'UP' and (lastAlertStatus['lastTime'] is False or lastAlertStatus['lastTime'] < repeatTime)):
    lastAlertStatus['state'] = state
    lastAlertStatus['lastTime'] = datetime.now()

    if len(mailText) > 0:
        msg = MIMEText(mailText)
        msg['Subject'] = mailSubject
        msg['From'] = mailFrom
        msg['To'] = mailTo

        print "Sending mail"
        s = smtplib.SMTP()
        s.connect()
        s.sendmail(mailFrom, [mailTo], msg.as_string())
        s.quit()
        print mailText

    f = open(stateFile, 'w')
    pickle.dump(lastAlertStatus, f)
    f.close()
