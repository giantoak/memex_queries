# memex_queries

This repository contains a Python module for calculating stats against a wide
variety of MEMEX escort data. It exists because MEMEX data is scattered
across multiple resources maintained by different teams  and knowledge sharing
is hard. `memex_queries` abstracts away as many of these data stores as
possible.

In the present implementation, this means that queries to MEMEX resources
created by different teams ([HBase] tables, the [Elasticsearch] CDR,
[Hive] and [Impala] data bases, all either *on* or *off* the MEMEX cluster)
are hidden away in low level functions within helper modules.
These functions can be used to compose high-level queries that cut across
these different data stores  are primarily concerne with data, not where
it has been stored.

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
Functions should  *generally* be named  along the lines of:

* `B_of_A_for_Y_of_X`
* `B_of_A_from_Y_of_X`
* `A_B_for_X_Y`
* `A_B_from_X_Y`

where:

* `B` is the data structure holding the output
* `A` is a reasonable shorthand for the desired output content.
* `Y` is a reasonable shorthand for the input data structure
* `X` is a reasonable shorthand for the output data structure.

`B` and `Y` may be omitted.

```
> # Sample high-level query using low-level components:
> def memex_query_three(image_cdr_id):
>   from helpers.cdr import cdr_ad_ids_for_cdr_image_ids
>   from helpers.hbase import df_of_dd_ids_for_cdr_ad_ids
>
>   cdr_ad_ids = cdr_ad_ids_for_cdr_image_ids(cdr_image_ids)
>   df = df_of_dd_ids_for_cdr_ad_ids(cdr_ad_ids)
>   return list(data_related_to_ads)
```

Helper functions are broken down across subpackages for each team:

#### [`memex_queries/helpers/giantoak/`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/giantoak)
Functions for interacting with resources provided by Giant Oak.
(Currently nil.)

#### [`memex_queries/helpers/isi/`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/isi)
Functions for interacting with resources provided by Columbia and ISI.
(Currently HBase tables, should also include an Elasticsearch instance.)

#### [`memex_queries/helpers/ist/`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/ist)
Functions for interacting with resources provided by IST.
(Currently the CDR.)

#### [`memex_queries/helpers/jpl/`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/jpl)
Functions for interacting with resources provided by JPL.
(Currently nil, should include Solr / Image Space.)

#### [`memex_queries/helpers/uncharted/`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/giantoak)
Functions for interacting with resources provided by Uncharted.
(Currently includes an HBase table.)

#### [`memex_queries/helpers/__init__.py`](https://github.com/giantoak/memex_queries/blob/master/memex_queries/helpers/__init__.py)
Functions that interact with multiple data stores provided by different teams.

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
Hashed CDR Image ID | A notional `_id`; the set of all CDR Image IDs for images that hash to the same value.
Clustered CDR Image ID | A notional `_id`; the set of all CDR Image IDs for images that have been clustered together. 
This could be because of an identical hash, or because they score highly on some other metric.
DD ID | The ID of an advertisement in the most recent dump of the Deep Dive Data, a.k.a Lattice Data, a.ka. Stanford Data.

# Future Plans

This is v.0.0.1 of this product, and was initially developed to facilitate
image comparisons by Giant Oak. *Future* versions of this library should put
the general problem of connecting data sources first. That would hopefully limit some amount of growth
in the number of generic functions being created.

[HBase]: https://hbase.apache.org/ "Apache HBase"
[Elasticsearch]: https://www.elastic.co/products/elasticsearch "Elastic Elasticsearch"
[Hive]: https://hive.apache.org/ "Apache Hive"
[Impala]: http://impala.io/ "Cloudera Impala"
[SQLite]: https://www.sqlite.org "SQLite"
[Pandas]: http://pandas.pydata.org/ "pandas"
[blaze]: http://blaze.pydata.org/ "blaze"