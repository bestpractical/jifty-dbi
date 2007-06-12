#!/usr/bin/env perl -w

use strict;
use warnings;
use File::Spec;
use Test::More;
BEGIN { require "t/utils.pl" }

use constant TESTS_PER_DRIVER => 35;

our (@available_drivers);
my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

foreach my $d (@available_drivers) {
SKIP: {
        unless ( has_schema( 'TestApp::Address', $d ) ) {
            skip "No schema for '$d' driver", TESTS_PER_DRIVER;
        }
        unless ( should_test($d) ) {
            skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
        }

        my $handle = get_handle($d);
        connect_handle($handle);
        isa_ok( $handle->dbh, 'DBI::db' );

        {my $ret = init_schema( 'TestApp::Address', $handle );
        isa_ok( $ret, 'DBI::st',
            "Inserted the schema. got a statement handle back" );}

        {    # simple, load the same thing from cache
            my $rec = TestApp::Address->new( handle => $handle );
            isa_ok( $rec, 'Jifty::DBI::Record' );

            my ($id)
                = $rec->create( Name => 'Jesse', Phone => '617 124 567' );
            ok( $id, "Created record #$id" );

            ok( $rec->load($id), "Loaded the record" );
            is( $rec->id, $id, "The record has its id" );
            is( $rec->name, 'Jesse', "The record's name is Jesse" );

            my $rec_cache = TestApp::Address->new( handle => $handle );
            my ( $status, $msg ) = $rec_cache->load_by_cols( id => $id );
            ok( $status, 'loaded record' );
            is( $rec_cache->id, $id, 'the same record as we created' );
            is( $msg, 'Fetched from cache', 'we fetched record from cache' );
        }

        Jifty::DBI::Record::Cachable->flush_cache;

        {    # load by name then load by id, check that we fetch from hash
            my $rec = TestApp::Address->new( handle => $handle );
            ok( $rec->load_by_cols( Name => 'Jesse' ), "Loaded the record" );
            is( $rec->name, 'Jesse', "The record's name is Jesse" );

            my $rec_cache = TestApp::Address->new( handle => $handle );
            my ( $status, $msg ) = $rec_cache->load_by_cols( id => $rec->id );
            ok( $status, 'loaded record' );
            is( $rec_cache->id, $rec->id, 'the same record as we created' );
            is( $msg, 'Fetched from cache', 'we fetched record from cache' );
        }

        Jifty::DBI::Record::Cachable->flush_cache;

        {    # load_by_cols and undef, 0 or '' values
            my $rec = TestApp::Address->new( handle => $handle );
            my ($id) = $rec->create( Name => 'EmptyPhone', Phone => '' );
            ok( $id, "Created record #$id" );
            ($id) = $rec->create( Name => 'ZeroPhone', Phone => 0 );
            ok( $id, "Created record #$id" );
            ($id) = $rec->create( Name => 'UndefPhone', Phone => undef );
            ok( $id, "Created record #$id" );

            Jifty::DBI::Record::Cachable->flush_cache;

            ok( $rec->load_by_cols( Phone => undef ), "Loaded the record" );
            is( $rec->name, 'UndefPhone', "UndefPhone record" );

            is( $rec->phone, undef, "Phone number is undefined" );

            ok( $rec->load_by_cols( Phone => '' ), "Loaded the record" );
            is( $rec->name,  'EmptyPhone', "EmptyPhone record" );
            is( $rec->phone, '',           "Phone number is empty string" );

            ok( $rec->load_by_cols( Phone => 0 ), "Loaded the record" );
            is( $rec->name,  'ZeroPhone', "ZeroPhone record" );
            is( $rec->phone, 0,           "Phone number is zero" );

     # XXX: next thing fails, looks like operator is mandatory
     # ok($rec->load_by_cols( Phone => { value => 0 } ), "Loaded the record");
            ok( $rec->load_by_cols(
                    Phone => { operator => '=', value => 0 }
                ),
                "Loaded the record"
            );
            is( $rec->name,  'ZeroPhone', "ZeroPhone record" );
            is( $rec->phone, 0,           "Phone number is zero" );
        }

        Jifty::DBI::Record::Cachable->flush_cache;

        {    # case insensetive columns names
            my $rec = TestApp::Address->new( handle => $handle );
            ok( $rec->load_by_cols( Name => 'Jesse' ), "Loaded the record" );
            is( $rec->name, 'Jesse', "loaded record" );

            my $rec_cache = TestApp::Address->new( handle => $handle );
            my ( $status, $msg )
                = $rec_cache->load_by_cols( name => 'Jesse' );
            ok( $status, 'loaded record' );
            is( $rec_cache->id, $rec->id, 'the same record as we created' );
            is( $msg, 'Fetched from cache', 'we fetched record from cache' );
        }

        Jifty::DBI::Record::Cachable->flush_cache;

        cleanup_schema( 'TestApp::Address', $handle );
    }
}    # SKIP, foreach blocks

1;

package TestApp::Address;
use base qw/Jifty::DBI::Record::Cachable/;

sub schema_mysql {
    <<EOF;
CREATE TEMPORARY table addresses (
        id integer AUTO_INCREMENT,
        name varchar(36),
        phone varchar(18),
        address varchar(50),
        employee_id int(8),
        PRIMARY KEY (id))
EOF

}

sub schema_pg {
    <<EOF;
CREATE TEMPORARY table addresses (
        id serial PRIMARY KEY,
        name varchar,
        phone varchar,
        address varchar,
        employee_id integer
)
EOF

}

sub schema_sqlite {

    <<EOF;
CREATE table addresses (
        id  integer primary key,
        name varchar(36),
        phone varchar(18),
        address varchar(50),
        employee_id int(8))
EOF

}

sub schema_oracle { [
    "CREATE SEQUENCE addresses_seq",
    "CREATE TABLE addresses (
        id integer CONSTRAINT addresses_key PRIMARY KEY,
        name varchar(36),
        phone varchar(18),
        employee_id integer
    )",
] }

sub cleanup_schema_oracle { [
    "DROP SEQUENCE addresses_seq",
    "DROP TABLE addresses", 
] }

1;

package TestApp::Address;

BEGIN {
    use Jifty::DBI::Schema;

    use Jifty::DBI::Record schema {
    column name => type is 'varchar(14)';

    column phone => type is 'varchar(18)';

    column
        address => type is 'varchar(50)',
        default is '';

    column employee_id => type is 'int(8)';
    }
}
1;
