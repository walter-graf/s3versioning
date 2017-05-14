#!/usr/bin/env python
# -*- coding: utf-8 -*-

# s3lisdv
#
# by Walter Graf
#
# usage: s3lisdv.py [-h] [-c s3-config-file] [--output csv-file-output]
#                  [--before yyyy-mm-ddThh:mm:ss] [--prefix object-prefix]
#                  bucket-name
#
# list all versions of a deleted object for a particular bucket
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
#   --before yyyy-mm-ddThh:mm:ss
#                         only list objects deleted before this time
#   --prefix object-prefix
#                         only list objects starting with this prefix

import sys
import os
import argparse
import ConfigParser
import time
import boto
import boto.s3.connection
import csv
import s3version

mod_time_in_sec = lambda s : time.mktime(time.strptime(s[0:s.find(".")],"%Y-%m-%dT%H:%M:%S"))
 
# parse command line arguments

parser = argparse.ArgumentParser(description = "list all versions of a deleted object for a particular bucket")
parser.add_argument("bucket", metavar="bucket-name", help="name of bucket")
parser.add_argument("-c", dest="s3_conf", metavar="s3-config-file", default=s3version.S3_CONF, help="use this S3 configuration file (default: %(default)s)")
parser.add_argument("--output", "-o", metavar="csv-file-output", type=argparse.FileType("wb"), default=sys.stdout, help="write csv output to this file (default: stdout)")
parser.add_argument("--before", metavar="yyyy-mm-ddThh:mm:ss", help="only list objects deleted before this time")
parser.add_argument("--prefix", metavar="object-prefix", help="only list objects starting with this prefix")
args = parser.parse_args()

s3_conf = args.s3_conf
bucket_name = args.bucket
if args.before == None:
  before_sec = time.time()
else:
  before_sec = time.mktime(time.strptime(args.before,"%Y-%m-%dT%H:%M:%S"))
prefix = args.prefix
output = args.output

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

# list all versions of deleted objects in bucket according to optional criteria

bucket = s3.get_bucket(bucket_name)
csv_dict["bucket"] = bucket_name

# loop over all object versions
# please note the extra logic because get_all_versions() can only handle 1000 versions at a time
# skip objects deleted after or equal specified time (mod_time >= before_sec)
# populate csv_dict and write to csv file

# to handle pagination
last_name = None
last_version_id = None

# to handle all versions belonging to one name
current_name = None

while True:
  versions = bucket.get_all_versions(prefix= prefix, key_marker=last_name, version_id_marker=last_version_id)

  for v in versions:
    if v.name != current_name:
      selected = False
      current_name = v.name
    has_del_marker = type(v) == boto.s3.deletemarker.DeleteMarker
    is_deleted = has_del_marker and v.is_latest
    if is_deleted and mod_time_in_sec(v.last_modified) < before_sec:
      selected = True
    if selected:
      print >> sys.stderr, "selected", v.name, v.version_id
      csv_dict["object"] = v.name
      csv_dict["version_id"] = v.version_id
      csv_dict["mod_time"] = v.last_modified
      if has_del_marker:
        csv_dict["size"] = "0"
        csv_dict["del_marker"] = "yes"
      else:
        csv_dict["size"] = str(v.size)
        csv_dict["del_marker"] = "no"
      csv_dict["is_latest"] = str(v.is_latest)
      csv_writer.writerow(csv_dict)
    last_name = v.name
    last_version_id = v.version_id

  if not versions.is_truncated:
    break
