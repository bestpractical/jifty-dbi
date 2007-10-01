#!/usr/bin/env perl -w

use strict;

use Test::More;
BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 9;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

use DateTime ();

foreach my $d ( @available_drivers ) {
SKIP: {
        unless( has_schema( 'TestApp::User', $d ) ) {
                skip "No schema for '$d' driver", TESTS_PER_DRIVER;
        }
        unless( should_test( $d ) ) {
                skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
        }
        diag("start testing with '$d' handle") if $ENV{TEST_VERBOSE};

        my $handle = get_handle( $d );
        connect_handle( $handle );
        isa_ok($handle->dbh, 'DBI::db');

        {my $ret = init_schema( 'TestApp::User', $handle );
        isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back" );}

        my $rec = TestApp::User->new( handle => $handle );
        isa_ok($rec, 'Jifty::DBI::Record');

        my ($id) = $rec->create( name => 'Foobar', interests => 'Slacking' );
        ok($id, "Successfuly created ticket");

	$rec->load_by_cols( name => 'foobar');
    TODO: {
        local $TODO = "How do we force mysql to be case sensitive?" if ( $d eq 'mysql' || $d eq 'mysqlPP' );
        is($rec->id, undef);
    }

	$rec->load_by_cols( name => { value => 'foobar', case_sensitive => 0, operator => '=' });
	is($rec->id, $id);

	$rec->load_by_cols( name => 'Foobar');
	is($rec->id, $id);

	$rec->load_by_cols( interests => 'slacking');
	is($rec->id, $id);;

	$rec->load_by_cols( interests => 'Slacking');
	is($rec->id, $id);;

        cleanup_schema( 'TestApp', $handle );
        disconnect_handle( $handle );
}
}

package TestApp::User;
use base qw/Jifty::DBI::Record/;

1;

sub schema_sqlite {

<<EOF;
CREATE table users (
        id integer primary key,
        name varchar,
        interests varchar
)
EOF

}

sub schema_mysql {

<<EOF;
CREATE TEMPORARY table users (
        id integer auto_increment primary key,
        name varchar(255),
        interests varchar(255)
)
EOF

}

sub schema_pg {

<<EOF;
CREATE TEMPORARY table users (
        id serial primary key,
        name varchar,
        interests varchar
)
EOF

}

use Jifty::DBI::Schema;

use Jifty::DBI::Record schema {
    column name      => type is 'varchar', label is 'Name', is case_sensitive;
    column interests => type is 'varchar';
};


1;

