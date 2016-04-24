# memex_queries

This repository contains a Python module for calculating stats against a wide
variety of MEMEX escort data. It exists because MEMEX data is scattered
across multiple resources and knowledge sharing remains hard. `memex_queries`
abstracts away as many of these data stores as possible.

In the present implementation, this means that queries to various MEMEX
[HBase] tables, the [Elasticsearch] CDR, and [Hive] and [Impala] for
useful datatables are hidden away in low level functions within helper
modules. These functions can be used to compose high-level queries that
are primarily concerne with data, not where it has been stored.

The module is organized as follows:

## Documentation
### [`docs/`](https://github.com/giantoak/memex_queries/tree/master/docs)
Folder of generated Sphinx docs. You can't get to them in your browser, but if
you clone the repository (or just download `docs/`) you should be able to
browse them from [`docs/index.html`](https://github.com/giantoak/memex_queries/blob/master/docs/index.html)

## Python stuff
### [`memex_queries/__init__.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/__init__.py)
A currently-empty `__init__.py` to make this a module. Families of queries
should live in their own sibling `.py` files.

### [`memex_queries/go_images.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/go_images.py)
Top-level queries to run against the data for info about images: these should
be used for answering questions related to specific fields or collections of fields:
```
> # Sample use:
> new_pd_series = pd_series.apply(memex_query_one)
> new_pd_dataframe = pd_dataframe.apply(memex_query_two)
```

### [`memex_queries/helpers`](https://github.com/giantoak/memex_queries/tree/master/memex_queries/helpers)
Lower-level helper functions that can be used as components of high-level
queries. These should facilitate hitting particular MEMEX resources.
DeepDive data is currently stored locally, within a [SQLite] database.
Functions should  *generally* be named  `X_for_Y` or `X_from_Y`, where `X` is
a reasonable approximation of the output data structure and `Y` is a
reasonable approximation of the input data structure.
```
> # Sample high-level query using low-level components:
> def memex_query_three(image_cdr_id):
>   from helpers.elasticsearch import get_data_from_cdr
>   from helpers.hbase import det_data_from_hbase
>
>   related_ads = data_from_cdr(image_cdr_id)
>   data_related_to_ads = data_fom_Hbase('some_table', related_ads, 'ad:field')
>   return list(data_related_to_ads)
```

Helper functions are broken down across four files:

#### [`memex_queries/helpers/__init__.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/__init__.py)
Functions that interact with mulitple data stores. We try to prioritise the
local SQLite DB first, then HBase, then the CDR / Elasticsearch.

#### [`memex_queries/helpers/cdr.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/cdr.py)
Functions for interacting with the CDR

#### [`memex_queries/helpers/hbase.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/hbase.py)
Functions for interacting with HBase

### [`memex_queries/helpers/sqlite.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/sqlite.py)
Functions for interacting with SQLite

## Setup stuff
### [`make_dd_sqlite_db.sql`](https://github.com/giantoak/memex_queries/blob/master/make_dd_sqlite_db.sql)
Load the Deep Dive dump into SQLite.
### [`get_dd_data_load_sb.sh`](https://github.com/giantoak/memex_queries/blob/master/get_dd_data_load_db.sh)
Download the Deep Dive dump and call `make_dd_sqlite_db.sql` to ingest it; see
[here](https://memexproxy.com/wiki/display/MPM/How+To+Get+Stanford+Memex+S3+Data)
for information about configuring your S3 access.


# Writing Queries
TSV files and CSV files are the bread-and-butter of data science. [Pandas] and
[blaze] are the bread-and-butter of data science in Python, and can be used to
convert data into TSVs and CSVs. As such, queries (or bulk versions of queries)
should be written with the expectation  that they either:
* Take data in bulk and…
  * return a `pandas.DataFrame`, `pandas.Series`, or appropriate `blaze` object.
  * write the results to a CSV or TSV and confirm that it has been written (or failed to be written)
* Take a single row of data and…
  * return a `pandas.DataFrame`, `pandas.Series`, or appropriate `blaze` object.
  * return a single row of data.


## "I don't like SQL and want to stay clear of it."
The easiest way to do this is create a `DataFrame` by selecting all of the
rows in the table:
```
> import pandas as pd
> from sqlalchemy import create_engine
> sqlite_connection = create_engine('s3:///{}'.format(path_to_sqlite_db))
> df = pd.DataFrame.read_sql('select * from some_table', sqlite_connection)
```

This isn't a terrible practice! (Using pandas to ingest a particularly large
table probably is.) Since all of the teams are performing their own
extractions, the design goal here should be to lean towards Python and away
from specific types of databases. If this gets you the data in the format you
want: great.

# Glossary of terms
Since we're jumping across a few different databases and munging together
identical objects with different identifiers, the module uses a particular
vernacular for clarity:

Term | Meaning
:--- |:---
CDR Ad ID | The `_id` of an advertisement in the CDR.
CDR Image ID | The `_id` of an image in the CDR. Each image has an ad as its parent
General CDR Image ID | A notional `_id`; the set of all CDR Image IDs for images that have identical hashes. (That is, that are actually the same image.
DD ID | The ID of an advertisement in the most recent dump of the Deep Dive Data, a.k.a Lattice Data, a.ka. Stanford Data.


[HBase]: https://hbase.apache.org/ "Apache HBase"
[Elasticsearch]: https://www.elastic.co/products/elasticsearch "Elastic Elasticsearch"
[Hive]: https://hive.apache.org/ "Apache Hive"
[Impala]: http://impala.io/ "Cloudera Impala"
[SQLite]: https://www.sqlite.org "SQLite"
[Pandas]: http://pandas.pydata.org/ "pandas"
[blaze]: http://blaze.pydata.org/ "blaze"