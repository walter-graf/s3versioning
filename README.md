#s3versioning

This repo contains a toolset of small Python programs that demonstrate
how the S3 versioning capability can be used to protect S3 object data
against accidental deletion and corruption.

It can be complemented by a fault tolerant and highly available or
even disaster resilient S3 platform like Ceph (in this concret case
Ceph works as a private cloud alternative to Amazon). 
The toolset has in fact been tested on a Ceph platform.

Both elements, this toolset and the resilient S3 platform it can run on,
form a full end to end data protection platform that makes traditional
backup obsolete.

The toolset in its current form allows

to make versioned buuckets

to turn versioning on for existing buckets

to list object versions that meet various criteria to narrow down the 
search for deleted and corrupted objects

to remove object versions based on a previously generated list with the
goal to get rid of corrupted object versions or undelete accidently
deleted objects

to perform housekeeping tasks like to limit the maximum number of
versions kept in the S3 archive

and more ...

However, it should be noted that the toolset today still has prototype
character and lacks certain  features as well as robustness in order to be
used for production.

In particular

no sufficient error handling has been implemented

no log file support has been added so far

the csv formatted output as interface between the identification of object
versions and its processing (mainly deleting corrupted versions or delete
markers) is not suitable for millions of objects. A performant database
could be the alternative

the toolset does only deal with limited meta data like timestamps but does
not (yet) allow user defined metadata as search criteria

additional security measures could be considered to before applying
critical and irreversible delete operations

no long term testing has been performed on the toolset

