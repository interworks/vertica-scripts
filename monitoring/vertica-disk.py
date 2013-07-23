#!/usr/bin/python

from subprocess import *
import smtplib
from email.mime.text import MIMEText

#######################################
## BEGIN configurable options
#######################################

vsql = '/opt/vertica/bin/vsql'
verticaHost = '192.168.1.1'
verticaUser = 'dbuser'
verticaPassword = 'dbpassword'

mailFrom = "no-reply@foo.com"
mailTo = "your-email@foo.com"
thresholdPct = 45.00

#######################################
## END configurable options
#######################################

mailText = ""
mailSubject = ""

qs = "SELECT CONCAT(CONCAT(node_name, ': '), TRIM('0' FROM free_pct::char(8))) FROM ("
qs += "SELECT node_name,"
qs += "  ROUND(SUM(disk_space_free_mb)/(SUM(disk_space_used_mb)+SUM(disk_space_free_mb))*100.0, 2) AS free_pct"
qs += "  FROM disk_storage "
qs += "  WHERE storage_usage ILIKE '%data%' AND storage_status = 'Active' GROUP BY node_name"
qs += ") d WHERE free_pct < %0.2f;" % (thresholdPct)

args = [vsql, '-Atq', '-h', verticaHost, '-U', verticaUser, '-w', verticaPassword, '-c', qs]
proc = Popen(args, stdout=PIPE)
results = proc.communicate()[0].splitlines()
ret = proc.returncode

if ret != 0:
    raise Exception("vsql returned error")


if len(results) > 0:
    mailText = "Disk space is low on the following node(s):\n\n" + "\n".join(results)
    mailSubject = "Vertica Alert: High disk utilization on %d node(s)" % (len(results))

if len(mailText) > 0:
    msg = MIMEText(mailText)
    msg['Subject'] = mailSubject
    msg['From'] = mailFrom
    msg['To'] = mailTo

    print "Sending mail:\n\n"
    print mailText
    
    s = smtplib.SMTP()
    s.connect()
    s.sendmail(mailFrom, [mailTo], msg.as_string())
    s.quit()
