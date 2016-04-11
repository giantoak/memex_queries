# memex_queries

This repository contains queries against various MEMEX HBase tables and the MEMEX CDR that return a variety of stats against individual images.
* `__init__.py` contains all of the top-level queries against the data.
* `HelperFuncs/__init__.py` contains helper functions that hit MEMEX resources (Hbase, Elastic) directly.

(Note that, at present, these queries are being optimized for single requests, **NOT** bulk searches. That is, each 
search for a particular image or ad will hit a sequence of HBase tables or the elastic index multiple times to find results. If we miss a hit on, say, an Hbase table, we immediately proceed to hit Elastic; we don't group together all of the misses and *then* hit Elastic. Perhaps we should.)
