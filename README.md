# memex_queries

This repository contains queries against various MEMEX HBase tables and the MEMEX CDR that return a variety of stats
against individual images.
* `__init__.py` contains all of the top-level queries against the data.
* `HelperFuncs/__init__.py` contains helper functions that hit MEMEX resources (Hbase, Elastic) directly.

(Note that, at present, these queries are being optimized for single requests, **NOT** bulk searches. That is, each 
search for a particular image or ad will hit a sequence of HBase tables or the elastic index multiple times to find results. If we miss a hit on, say, an HBase table, we immediately proceed to hit Elastic; we don't group together all of the misses and *then* hit
 Elastic. We should.)

TSV files and CSV files are the bread-and-butter of data science.
[Pandas](http://pandas.pydata.org/) and [blaze](http://blaze.pydata.org/) are the bread-and-butter of data science in
Python, and can be used to get things into TSVs and CSVs. As such, queries (or bulk versions of queries) should try to
either write files or return data frames.

In general, we're relying on data stored in the MEMEX cluster or in a local [sqlite](https://www.sqlite.org/org) 
database. I want fast results, and keeping everything in flat files has resulted in some very slow reads. Better 
workarounds would be welcome, but for now I'd prefer to dump stuff to pandas after pulling out rows of interest.

*That said*, as currently written the code tends to lean on pandas for calculation. SQL is used for pulling subset
of ad data, and pandas is for more sophisticated work. This is perhaps not the most efficient way of doing things,
but it hopefully makes SQL easier to swap out than pandas.


# Glossary of terms

Term | Meaning
:--- |:---
CDR Ad ID | The `_id` of an advertisement in the CDR.
CDR Image ID | The `_id` of an image in the CDR. Each image has an ad as its parent
General CDR Image ID | A notional `_id`; the set of all CDR Image IDs for images that have identical hashes. (That is, that are actually the same image.
DD ID | The ID of an advertisement in the most recent dump of the Deep Dive Data, a.k.a Lattice Data, a.ka. Stanford Data. (Available at s3://memex-data/escort_cdr_2; see [here](https://memexproxy.com/wiki/display/MPM/How+To+Get+Stanford+Memex+S3+Data) for information about access.)

