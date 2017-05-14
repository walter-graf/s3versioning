import os
S3_DEFAULT_CONF_FILE = "s3versioning.cnf"
S3_CONF = os.environ["HOME"] + "/" + S3_DEFAULT_CONF_FILE
S3_CSV_KEYS = [ "bucket", "object", "version_id", "mod_time", "size", "del_marker", "is_latest" ]
