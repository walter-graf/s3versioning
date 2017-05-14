#!/usr/bin/env python
# -*- coding: utf-8 -*-

# s3delov
#
# by Walter Graf
#
# usage: s3delov.py [-h] [-c s3-config-file] [--input csv-file-input]
#                   bucket-name
# 
# delete object versions according to a csv file
# 
# positional arguments:
#   bucket-name           bucket hosting the to be deleted versioned objects
# 
# optional arguments:
#   -h, --help            show this help message and exit
#   -c s3-config-file     use this S3 configuration file (default:
#                         $HOME/s3versioning.cnf)
#   --input csv-file-input, -i csv-file-input
#                         read csv input from this file (default: stdin)


import sys
import os
import argparse
import ConfigParser
import time
import boto
import boto.s3.connection
import csv
import s3version

# parse command line arguments

parser = argparse.ArgumentParser(description = "delete object versions according to a csv file")
parser.add_argument("bucket", metavar="bucket-name", help="bucket hosting the to be deleted versioned objects")
parser.add_argument("-c", dest="s3_conf", metavar="s3-config-file", default=s3version.S3_CONF, help="use this S3 configuration file (default: %(default)s)")
parser.add_argument("--input", "-i", metavar="csv-file-input", type=argparse.FileType("rb"), default=sys.stdin, help="read csv input from this file (default: stdin)")
args = parser.parse_args()

s3_conf = args.s3_conf
bucket_name = args.bucket
input = args.input

# parse config file

cnf = ConfigParser.RawConfigParser()
cnf.read(s3_conf)

access = cnf.get("connect", "access")
secret = cnf.get("connect", "secret")
host = cnf.get("connect", "host")
port = cnf.getint("connect", "port")
is_secure = cnf.getboolean("connect", "is_secure")

# establish S3 connection

s3 = boto.connect_s3(
  aws_access_key_id = access,
  aws_secret_access_key = secret,
  host = host,
  port = port,
  is_secure = is_secure,
  calling_format = boto.s3.connection.OrdinaryCallingFormat()
  )

# prepare csv input

csv_reader = csv.DictReader(input, delimiter=",", quotechar='"')

# delete object versions in bucket according to csv file

bucket = s3.get_bucket(bucket_name)

# loop over all csv file rows
# check bucket name and skip in case bucket name doesn't match
# delete versioned objects

for c in csv_reader:
  if c["bucket"] != bucket_name:
    print >> sys.stderr, "bucket name mismatch:", c["bucket"], "!=", bucket_name, "- skipping delete"
    continue
  print >> sys.stderr, "deleting", c["object"], c["version_id"]
  bucket.delete_key(c["object"], version_id = c["version_id"])
