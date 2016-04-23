#!/bin/bash
aws --profile=MemexReader s3 cp --recursive s3://memex-data/escort_cdr_2/ .
bunzip2 escort_cdr_2/cdr/age-combined.tsv.bz2
bunzip2 escort_cdr_2/cdr/cbsa-combined.tsv.bz2
bunzip2 escort_cdr_2/cdr/email-text-corrected.tsv.bz2
bunzip2 escort_cdr_2/cdr/ethnicities-dom-disagged.tsv.bz2
bunzip2 escort_cdr_2/cdr/flags.tsv.bz2
bunzip2 escort_cdr_2/cdr/massage_places-combined.tsv.bz2
bunzip2 escort_cdr_2/cdr/service-text.tsv.bz2
bunzip2 escort_cdr_2/cdr/rates-text-split.tsv.bz2
bunzip2 escort_cdr_2/cdr/phones-per-line.tsv.bz2
bunzip2 escort_cdr_2/cdr/post_date-dom_secs.tsv.bz2
bunzip2 escort_cdr_2/cdr/doc_id_mapping.tsv.bz2
sqlite3 dd_dump.db < make_dd_sqlite_db.sql
rm -r escort_cdr_2
