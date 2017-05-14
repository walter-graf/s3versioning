#!/usr/bin/env python
# -*- coding: utf-8 -*-

# s3delvb
#
# by Walter Graf
#
# usage: s3delvb.py [-h] [-c s3-config-file] [--yes-i-really-really-mean-it]
#                  bucket-name
#
# delete versioned bucket including its versioned objects
#
# positional arguments:
#   bucket-name           name of bucket to be deleted
#
# optional arguments:
#   -h, --help            show this help message and exit
#   -c s3-config-file     use this S3 configuration file (default:
#                         $HOME/s3versioning.cnf)
#   --yes-i-really-really-mean-it
#                         specify this option to enforce delete

import sys
import os
import argparse
import ConfigParser
import boto
import boto.s3.connection
import s3version

# parse command line arguments

parser = argparse.ArgumentParser(description = "delete versioned bucket including its versioned objects")
parser.add_argument("bucket", metavar="bucket-name", help="name of bucket to be deleted")
parser.add_argument("-c", dest="s3_conf", metavar="s3-config-file", default=s3version.S3_CONF, help="use this S3 configuration file (default: %(default)s)")
parser.add_argument("--yes-i-really-really-mean-it", dest="enforce", action="store_true", help="specify this option to enforce delete")
args = parser.parse_args()

s3_conf = args.s3_conf
bucket_name = args.bucket
enforce = args.enforce

# check if delete operation is really enforced

if not enforce:
  print >> sys.stderr, 'please specify option "--yes-i-really-really-mean-it" to delete the bucket including all its versioned objects'
  quit()

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

# delete versioned bucket

bucket = s3.get_bucket(bucket_name)

# first remove all versioned objects
# please note the extra logic because get_all_versions() can only handle 1000 versions at a time

print >> sys.stderr, "removing all versioned objects in bucket", bucket_name, ":"

last_name = None
last_version_id = None
while True:
  versions = bucket.get_all_versions(key_marker=last_name, version_id_marker=last_version_id)

  for v in versions:
    print >> sys.stderr, "removing", v.name, v.version_id
    bucket.delete_key(v.name, version_id = v.version_id)
    last_name = v.name
    last_version_id = v.version_id

  if not versions.is_truncated:
    break

print >> sys.stderr, "removing the now empty bucket", bucket.name
s3.delete_bucket(bucket.name)
