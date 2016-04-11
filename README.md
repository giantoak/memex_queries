# memex_queries

This repository contains queries against various MEMEX HBase tables and the MEMEX CDR that return a variety of stats against individual images.
* `__init__.py` contains all of the top-level queries against the data.
* `HelperFuncs/__init__.py` contains helper functions that hit MEMEX resources (Hbase, Elastic) directly.

(Note that, at present, these queries are being optimized for single requests, **NOT** bulk searches. That is, each 
search for a particular entity will try to hit a sequence of tables for the best results.
