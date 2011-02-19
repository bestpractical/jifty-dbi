#!/usr/bin/env perl -w

use strict;
use Test::More;
eval "use Test::Spelling";
plan skip_all => "Coverage tests only run for authors" unless (-d 'inc/.author');

plan skip_all => "Test::Spelling required for testing POD spelling" if $@;

add_stopwords(<DATA>);

local $ENV{LC_ALL} = 'C';
set_spell_cmd('aspell list -l en');

all_pod_files_spelling_ok();

__DATA__
Autocommit
autocompleted
backend
BYTEA
canonicalizer
canonicalizers
Checkbox
classdata
COLUMNNAME
Combobox
cpan
database's
datasource
DateTime
DBD
dbh
DBI
deserialize
dsn
formatter
Glasser
Hanenkamp
hashrefs
HookResults
Informix
Informix's
InlineButton
Jifty
Knopp
LLC
login
lookups
lossy
marshalling
memcached
metadata
mhat
mixin
mixins
MyModel
myscript
mysql's
NULLs
ODBC
OtherClass
OtherCollection
paramhash
Postgres
postgres
PostgreSQL
prefetch
prefetched
prefetches
preload
prepends
PrintError
QUERYSTRING
RaiseError
recordset
RequireSSL
requiressl
resultsets
Ruslan
SchemaGenerator
SearchBuilder
sid
Spier
SQL
SQLite
SQLite's
STATEMENTREF
STDERR
Storable
Sybase
Sybase's
Syck
TABLENAME
Tappe
TODO
unimported
unlimit
unmarshalling
Unrendered
username
UTC
UTF
utf
validator
validators
Vandiver
wildcard
YAML
Zakirov
