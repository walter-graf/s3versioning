#!/usr/bin/env python
# -*- coding: utf-8 -*-

# s3listv
#
# by Walter Graf
#
# usage: s3listv.py [-h] [-c s3-config-file] [--output csv-file-output]
#                   [--prefix object-prefix] [--version-limit version-limit]
#                   bucket-name
#
# list truncated versions for a particular bucket
#
# positional arguments:
#   bucket-name           name of bucket
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -c s3-config-file     use this S3 configuration file (default:
#                         $HOME/s3versioning.cnf)
#   --output csv-file-output, -o csv-file-output
#                         write csv output to this file (default: stdout)
#   --prefix object-prefix
#                         only list objects starting with this prefix
#   --version-limit version-limit
#                         list all versions exceeding the specified limit

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

parser = argparse.ArgumentParser(description = "list truncated versions for a particular bucket")
parser.add_argument("bucket", metavar="bucket-name", help="name of bucket")
parser.add_argument("-c", dest="s3_conf", metavar="s3-config-file", default=s3version.S3_CONF, help="use this S3 configuration file (default: %(default)s)")
parser.add_argument("--output", "-o", metavar="csv-file-output", type=argparse.FileType("wb"), default=sys.stdout, help="write csv output to this file (default: stdout)")
parser.add_argument("--prefix", metavar="object-prefix", help="only list objects starting with this prefix")
parser.add_argument("--version-limit", metavar="version-limit", help="list all versions exceeding the specified limit")
args = parser.parse_args()

s3_conf = args.s3_conf
bucket_name = args.bucket
prefix = args.prefix
output = args.output
version_limit = int(args.version_limit)

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

# prepare csv output

csv_dict = {}
csv_writer = csv.DictWriter(output, s3version.S3_CSV_KEYS, delimiter=",", quotechar='"')

#csv_writer.writeheader()
for k in s3version.S3_CSV_KEYS:
  csv_dict[k] = k
csv_writer.writerow(csv_dict)

# list object versions in bucket exceeding the specified version limit

bucket = s3.get_bucket(bucket_name)
csv_dict["bucket"] = bucket_name

# loop over all object versions matching the specified prefix
# please note the extra logic because get_all_versions() can only handle 1000 versions at a time
# count versions
# do not count delete markers
# for all versions beyond version limit populate csv_dict and write to csv file

# to handle pagination
last_name = None
last_version_id = None

# to handle correct version count
current_name = None

while True:
  versions = bucket.get_all_versions(prefix= prefix, key_marker=last_name, version_id_marker=last_version_id)

  for v in versions:
    if v.name != current_name:
      vcount = 0
      current_name = v.name
    has_no_del_marker = type(v) != boto.s3.deletemarker.DeleteMarker
    if has_no_del_marker:
      vcount += 1
    if vcount > version_limit:
      print >> sys.stderr, "selected for truncation", v.name, v.version_id
      csv_dict["object"] = v.name
      csv_dict["version_id"] = v.version_id
      csv_dict["mod_time"] = v.last_modified
      if has_no_del_marker:
        csv_dict["size"] = str(v.size)
        csv_dict["del_marker"] = "no"
      else:
        csv_dict["size"] = "0"
        csv_dict["del_marker"] = "yes"
      csv_dict["is_latest"] = str(v.is_latest)
      csv_writer.writerow(csv_dict)
    last_name = v.name
    last_version_id = v.version_id

  if not versions.is_truncated:
    break
