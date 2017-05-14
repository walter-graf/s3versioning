#!/usr/bin/env python
# -*- coding: utf-8 -*-

# s3makvb
#
# by Walter Graf
#
# usage: s3makvb.py [-h] [-c s3-config-file] bucket-name
#
# make versioned bucket
#
# positional arguments:
#   bucket-name        bucket name
#
# optional arguments:
#   -h, --help         show this help message and exit
#   -c s3-config-file  use this S3 configuration file (default:
#                      $HOME/s3versioning.cnf)

import sys
import os
import argparse
import ConfigParser
import boto
import boto.s3.connection
import s3version

# parse command line arguments

parser = argparse.ArgumentParser(description = "make versioned bucket")
parser.add_argument("bucket", metavar="bucket-name", help="bucket name")
parser.add_argument("-c", dest="s3_conf", metavar="s3-config-file", default=s3version.S3_CONF, help="use this S3 configuration file (default: %(default)s)")
args = parser.parse_args()

s3_conf = args.s3_conf
bucket_name = args.bucket

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

# make versioned bucket

print >> sys.stderr, "making bucket", bucket_name
bucket = s3.create_bucket(bucket_name)
print >> sys.stderr, "Enabling versioning"
bucket.configure_versioning(True)
