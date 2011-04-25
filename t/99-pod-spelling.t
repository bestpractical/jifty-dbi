use strict;
use warnings;

use Test::More;
BEGIN {
    plan skip_all => "Spelling tests only run for authors"
        unless -d 'inc/.author';
}

eval "use Test::Spelling 0.12";
plan skip_all => "Test::Spelling 0.12 required for testing POD spelling" if $@;

add_stopwords(<DATA>);

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
