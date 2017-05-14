#!/usr/bin/env python
# -*- coding: utf-8 -*-

# s3lisov
#
# by Walter Graf
#
# usage: s3lisov.py [-h] [-c s3-config-file] [--output csv-file-output]
#                   [--after yyyy-mm-ddThh:mm:ss] [--prefix object-prefix]
#                   [--only-deleted | --no-deleted]
#                   bucket-name
#
# list object versions for a particular bucket
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
#   --after yyyy-mm-ddThh:mm:ss
#                         only list objects modified after this time
#   --prefix object-prefix
#                         only list objects starting with this prefix
#   --only-deleted        only list deleted objects
#   --no-deleted          exclude deleted objects from list

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

parser = argparse.ArgumentParser(description = "list object versions for a particular bucket")
parser.add_argument("bucket", metavar="bucket-name", help="name of bucket")
parser.add_argument("-c", dest="s3_conf", metavar="s3-config-file", default=s3version.S3_CONF, help="use this S3 configuration file (default: %(default)s)")
parser.add_argument("--output", "-o", metavar="csv-file-output", type=argparse.FileType("wb"), default=sys.stdout, help="write csv output to this file (default: stdout)")
parser.add_argument("--after", metavar="yyyy-mm-ddThh:mm:ss", help="only list objects modified after this time")
parser.add_argument("--prefix", metavar="object-prefix", help="only list objects starting with this prefix")
arggroup=parser.add_mutually_exclusive_group()
arggroup.add_argument("--only-deleted", action="store_true", help="only list deleted objects")
arggroup.add_argument("--no-deleted", action="store_true", help="exclude deleted objects from list")
args = parser.parse_args()

s3_conf = args.s3_conf
bucket_name = args.bucket
if args.after == None:
  after_sec = 0.0
else:
  after_sec = time.mktime(time.strptime(args.after,"%Y-%m-%dT%H:%M:%S"))
prefix = args.prefix
output = args.output
only_deleted = args.only_deleted
no_deleted = args.no_deleted

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

# list object versions in  bucket according to optional criteria

bucket = s3.get_bucket(bucket_name)
csv_dict["bucket"] = bucket_name

# loop over all object versions
# please note the extra logic because get_all_versions() can only handle 1000 versions at a time
# skip objects with mod_time before or equal to after_sec
# skip existing (not deleted) objects in case of only_deleted
# skip deleted objects in case of no_deleted
# populate csv_dict and write to csv file

last_name = None
last_version_id = None
while True:
  versions = bucket.get_all_versions(prefix= prefix, key_marker=last_name, version_id_marker=last_version_id)

  for v in versions:
    if mod_time_in_sec(v.last_modified) <= after_sec:
      continue
    has_del_marker = type(v) == boto.s3.deletemarker.DeleteMarker
    is_deleted = has_del_marker and v.is_latest
    if only_deleted:
      if not is_deleted:
        continue
    if no_deleted:
       if is_deleted:
        continue
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

