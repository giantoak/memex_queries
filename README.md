# memex_queries

This repository contains a module calculate stats against a wide variety of MEMEX escort data. It is organized as follows:

## Python stuff
### [`memex_queries/__init__.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/__init__.py)
Top-level queries to run against the data: these should be used for answering questions related to specific fields or collections of fields:
```
> # Sample use:
> new_pd_series = pd_series.apply(memex_query_one)
> new_pd_dataframe = pd_dataframe.apply(memex_query_two)
```

### [`memex_queries/HelperFuncs`](https://github.com/giantoak/memex_queries/tree/master/memex_queries/HelperFuncs)
Lower-level helper functions that can be used as components of high-level queries. These should facilitate hitting particular MEMEX resources ([Elasticsearch,](https://www.elastic.co/products/elasticsearch) [HBase,](https://hbase.apache.org/) and perhaps eventually [Hive](https://hive.apache.org/) and [Impala](http://impala.io/)). DeepDive data is currently stored locally, within a [SQLite](https://www.sqlite.org) database.
```
> # Sample high-level query using low-level components:
> def memex_query_three(image_cdr_id):
>   related_ads = get_data_from_cdr(image_cdr_id)
>   data_related_to_ads = get_data_from_hbase('some_table', related_ads)
>   return list(data_related_to_ads)
```

## Setup stuff
### [`make_dd_sqlite_db.sql`](https://github.com/giantoak/memex_queries/blob/master/make_dd_sqlite_db.sql)
Load the Deep Dive dump into SQLite.
### [`get_dd_data_load_sb.sh`](https://github.com/giantoak/memex_queries/blob/master/get_dd_data_load_db.sh)
Download the Deep Dive dump and call `make_dd_sqlite_db.sql` to ingest it; see [here](https://memexproxy.com/wiki/display/MPM/How+To+Get+Stanford+Memex+S3+Data) for information about configuring your S3 access.


# Writing Queries
TSV files and CSV files are the bread-and-butter of data science.
[Pandas](http://pandas.pydata.org/) and [blaze](http://blaze.pydata.org/) are the bread-and-butter of data science in
Python, and can be used to get things into TSVs and CSVs. As such, queries (or bulk versions of queries) should be written with the expectation that they either:
* Take data in bulk and…
  * return a `pandas.DataFrame`, `pandas.Series`, or appropriate `blaze` object.
  * write the results to a CSV or TSV and confirm that it has been written (or failed to be written)
* Take a single row of data and…
  * return a `pandas.DataFrame`, `pandas.Series`, or appropriate `blaze` object.
  * return a single row of data.


## "I don't like SQL and want to stay clear of it."
The easiest way to do this is create a `DataFrame` by selecting all of the rows in the table:
  ```
  > import pandas as pd
  > from sqlalchemy import create_engine
  > sqlite_connection = create_engine('s3:///{}'.format(path_to_sqlite_db))
  > df = pd.DataFrame.read_sql('select * from some_table', sqlite_connection)
  ```

This is *not* a terrible practice, though using pandas to ingest a particularly large table may be. Since all of the teams are performing their own extractions, the design goal here should be to lean towards Python and away from specific types of databases. If this gets you the data in the format you want, great.

# Glossary of terms
Since we're jumping across a few different databases and munging together identical objects with different identifiers, I use a particular vernacular for clarity

Term | Meaning
:--- |:---
CDR Ad ID | The `_id` of an advertisement in the CDR.
CDR Image ID | The `_id` of an image in the CDR. Each image has an ad as its parent
General CDR Image ID | A notional `_id`; the set of all CDR Image IDs for images that have identical hashes. (That is, that are actually the same image.
DD ID | The ID of an advertisement in the most recent dump of the Deep Dive Data, a.k.a Lattice Data, a.ka. Stanford Data.
