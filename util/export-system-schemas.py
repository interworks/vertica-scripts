#!/usr/bin/env python

# This script is useful for exporting DDLs that you can use to recreate
# all of the Vertica system tables in another schema, or in a Postgres
# database. This is useful to then import a Vertica diagdump for 
# analysis.
#
# Some of the tables exported are actually views, but for simplicity's
# sake, we just treat all system tables & views as tables. This is how
# Vertica's collect_diag_dump.sh script also behaves.

import time
import os
from subprocess import *
from sys import argv
import re

# If building for importing into Postgres, the following
# tables contain column names not allowed in Postgres
excludeTables = [
    'vs_catalog_event_rebalance',
    'vs_catalog_event_restore'
]

vsqlCmd = '/opt/vertica/bin/vsql'
verticaHost = '192.168.1.1'
verticaUser = 'dbuser'
verticaPassword = 'dbpassword'
defaultFieldDelim = "|"
defaultRecDelim = "\n"

os.environ['LANG'] = 'en_US.UTF-8'

def vsql(qs):
    global vsqlCmd
    global verticaHost
    global verticaUser
    global verticaPassword
    global defaultFieldDelim
    global defaultRecDelim

    args = [vsqlCmd, '-Atq', '-P', 'footer', '-h', verticaHost, '-U', verticaUser, '-w', verticaPassword]
    args += ['-F', defaultFieldDelim, '-R', defaultRecDelim]
    args += ['-c', qs]

    proc = Popen(args, stdout=PIPE)
    results = proc.communicate()[0].strip()
    ret = proc.returncode

    if ret != 0:
        raise Exception("vsql returned error")

    return results

def main():
    global defaultFieldDelim
    global defaultRecDelim

    # vertVersion = vsql("SELECT REGEXP_REPLACE(REGEXP_REPLACE(VERSION(), '^[\w\s]+v([\d\.\-]+)$', '\1'), '\D', '_');")
    # schemaFileName = "schema_%s.sql" % (vertVersion)
    # f = open(schemaFileName, 'w')

    vertVersion = vsql("SELECT VERSION();")
    print "-- VERSION(): %s" % (vertVersion)

    qs = "SELECT table_schema, table_name FROM vs_system_tables "

    if len(excludeTables):
        qs += " WHERE table_name NOT IN ('" + ("', '".join(excludeTables)) + "')"

    qs += " ORDER BY table_schema, table_name"

    tables = vsql(qs).split(defaultRecDelim)

    vsql("CREATE SCHEMA __dd_temp")

    ctre = re.compile('^CREATE TABLE __dd_temp\.')

    for table in tables:
        table = table.split(defaultFieldDelim)
        schema = table[0]
        table = table[1]
        tempTableName = "out_%s_%s" % (schema, table)

        print "-- %s.%s" % (schema, table)

        vsql("CREATE TABLE __dd_temp.%s AS SELECT * FROM %s.%s LIMIT 0;" % (tempTableName, schema, table))

        qs = "SELECT EXPORT_TABLES('', '__dd_temp.%s');" % (tempTableName)
        result = vsql(qs)
        result = ctre.sub('CREATE TABLE diagdump.', result)
        print result
        # f.write(vsql(qs) + "\n")


    # f.close()

    vsql("DROP SCHEMA __dd_temp CASCADE")

main()