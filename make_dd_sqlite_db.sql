.mode tabs

create table if not exists dd_id_to_age (dd_id int, age int);
.import cdr_escorts_2/cdr/age-combined.tsv dd_id_to_age
create index dd_age_ix on dd_id_to_age(dd_id);

create table if not exists dd_id_to_cbsa (dd_id int, area text, area_type text);
.import cdr_escorts_2/cdr/cbsa-combined.tsv dd_id_to_cbsa
create index dd_cbsa_ix  on dd_id_to_cbsa (dd_id);
create index area_type_cbsa_ix on dd_id_to_cbsa (area_type);

create table if not exists dd_id_to_email (dd_id int, email text);
.import cdr_escorts_2/cdr/email-text-corrected.tsv dd_id_to_email
create index dd_email_ix on dd_id_to_email (dd_id);
create index email_dd_ix on dd_id_to_email (email);

create table if not exists dd_id_to_ethnicity (dd_id int, ethnicity text);
.import cdr_escorts_2/cdr/ethnicities-dom-disagged.tsv dd_id_to_ethnicity
create index dd_ethnicity_ix on dd_id_to_ethnicity (dd_id);
create index ethnicity_dd_ix on dd_id_to_ethnicity (ethnicity);

create table if not exists dd_id_to_flag (dd_id int, flag text);
.import cdr_escorts_2/cdr/flags.tsv dd_id_to_flag
create index dd_flag_ix on dd_id_to_flag (dd_id);
create index flag_dd_ix on dd_id_to_flag (flag);

create table if not exists dd_id_to_parlor (dd_id int, url text, phone text, parlor_name text, domain text);
.import cdr_escorts_2/cdr/massage_places-combined.tsv dd_id_to_parlor
create index dd_id_parlor_ix on dd_id_to_parlor (dd_id);

create table if not exists dd_id_to_service (dd_id int primary key, service text) without rowid;
.import cdr_escorts_2/cdr/service-text.tsv dd_id_to_service
create index service_dd_ix  on dd_id_to_service (service);

create table if not exists dd_id_to_price_duration (dd_id int, price int, duration_in_mins int);
.import cdr_escorts_2/cdr/rates-text-split.tsv dd_id_to_price_duration
create index dd_price_dur_ix on dd_id_to_price_duration (dd_id);
create index price_dd_dur_ix on dd_id_to_price_duration (price);
create index dur_dd_price_ix on dd_id_to_price_duration (duration_in_mins);

create table if not exists dd_id_to_phone (dd_id int,  phone text);
.import cdr_escorts_2/cdr/phones-per-line.tsv dd_id_to_phone
create index dd_phone_ix on dd_id_to_phone (dd_id);
create index phone_dd_ix on dd_id_to_phone (phone);

create table if not exists dd_id_to_post_date (dd_id, post_date text);
.import cdr_escorts_2/cdr/post_date-dom_secs.tsv dd_id_to_post_date
create index dd_post_date_ix on dd_id_to_post_date (dd_id);
create index post_date_dd_ix on dd_id_to_post_date (post_date);

# Ideally, this table should be created as:
# create table if not exists dd_id_to_cdr_id (dd_id int primary key, cdr_id text unique not null) without rowid;
# However, this was causing *massive* time outs. Unclear why
# As such, we simply take it on faith that that these have a 1-to-1 match.
# If they don't, we're in trouble.
create table if not exists dd_id_to_cdr_id (dd_id int, cdr_id text not null);
.import cdr_escorts_2/cdr/doc_id_mapping.tsv dd_id_to_cdr_id
create index cdr_id_ix on dd_id_to_cdr_id (cdr_id);
create index dd_id_ix on dd_id_to_cdr_id (dd_id);
