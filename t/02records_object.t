#!/usr/bin/env perl -w


use strict;
use warnings;
use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 9;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

foreach my $d ( @available_drivers ) {
SKIP: {
        unless( has_schema( 'TestApp', $d ) ) {
                skip "No schema for '$d' driver", TESTS_PER_DRIVER;
        }
        unless( should_test( $d ) ) {
                skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
        }

        my $handle = get_handle( $d );
        connect_handle( $handle );
        isa_ok($handle->dbh, 'DBI::db');

        my $ret = init_schema( 'TestApp', $handle );
        isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back");

        my $emp = TestApp::Employee->new( handle => $handle );
        my $e_id = $emp->create( Name => 'RUZ' );
        ok($e_id, "Got an id for the new emplyee");
        my $phone = TestApp::Phone->new( handle => $handle );
        isa_ok( $phone, 'TestApp::Phone', "it's a TestApp::Phone");
        my $p_id = $phone->create( employee => $e_id, phone => '+7(903)264-03-51');
        # XXX: test fails if next string is commented
        is($p_id, 1, "Loaded record $p_id");
        $phone->load( $p_id );

        my $obj = $phone->employee( handle => $handle );
        ok($obj, "Employee #$e_id has phone #$p_id");
        isa_ok( $obj, 'TestApp::Employee');
        is($obj->id, $e_id);
        is($obj->name, 'RUZ');

        cleanup_schema( 'TestApp', $handle );
        disconnect_handle( $handle );
}} # SKIP, foreach blocks

1;


package TestApp;
sub schema_sqlite {
[
q{
CREATE table employees (
        id integer primary key,
        name varchar(36)
)
}, q{
CREATE table phones (
        id integer primary key,
        employee integer NOT NULL,
        phone varchar(18)
) }
]
}

sub schema_mysql {
[ q{
CREATE TEMPORARY table employees (
        id integer AUTO_INCREMENT primary key,
        name varchar(36)
)
}, q{
CREATE TEMPORARY table phones (
        id integer AUTO_INCREMENT primary key,
        employee integer NOT NULL,
        phone varchar(18)
)
} ]
}

sub schema_pg {
[ q{
CREATE TEMPORARY table employees (
        id serial PRIMARY KEY,
        name varchar
)
}, q{
CREATE TEMPORARY table phones (
        id serial PRIMARY KEY,
        employee integer references employees(id),
        phone varchar
)
} ]
}



package TestApp::Employee;
use base qw/Jifty::DBI::Record/;

1;

package TestApp::Employee::Schema;
BEGIN {
    use Jifty::DBI::Schema;

    column name => type is 'varchar(18)';
}

1;



package TestApp::Phone;
use base qw/Jifty::DBI::Record/;

1;

package TestApp::Phone::Schema;
BEGIN {
    use Jifty::DBI::Schema;

    column employee => refers_to TestApp::Employee;
    column phone    => type 'varchar(18)';
}

1;
