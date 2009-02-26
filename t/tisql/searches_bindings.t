#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 8;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

use Data::Dumper;

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
    isa_ok($ret, 'DBI::st', "Inserted the schema. got a statement handle back");

    my $count_users = init_data( 'TestApp::User', $handle );
    ok( $count_users,  "init users data" );

    my $clean_obj = TestApp::UserCollection->new( handle => $handle );
    my $users_obj = $clean_obj->clone;
    is_deeply( $users_obj, $clean_obj, 'after Clone looks the same');

    run_our_cool_tests(
        $users_obj,
        [".login = ?", 'a'] => { 'aa' => 1, 'ab' => 1, 'ac' => 1 },
        [".login != ?", 'a'] => { 'ba' => 1, 'bb' => 1, 'bc' => 1, 'ca' => 1, 'cb' => 1, 'cc' => 1 },

    );

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks


sub run_our_cool_tests {
    my $collection = shift;
    my @tests = @_;
    while (@tests) {
        my ($q, $check) = splice( @tests, 0, 2 );
        $collection->clean_slate;
        $collection->tisql->query( @$q );
        my $expected_count = scalar grep $_, values %$check;
        is($collection->count, $expected_count, "count is correct for ". $q->[0])
            or diag "wrong count query: ". $collection->build_select_count_query;
       
        my @not_expected;
        while (my $item = $collection->next ) {
            my $t = $item->test;
            push @not_expected, $t unless $check->{ $t };
            delete $check->{ $t };
        }
        ok !@not_expected, 'didnt find additionals'
            or diag "wrong query: ". $collection->build_select_query;
    }
}
1;


package TestApp;
sub schema_sqlite {
[
q{
CREATE table users (
    id integer primary key,
    test varchar(36),
    login varchar(36),
    name varchar(36)
) },
]
}

sub schema_mysql {
[
q{
CREATE TEMPORARY table users (
    id integer primary key AUTO_INCREMENT,
    test varchar(36),
    login varchar(36),
    name varchar(36)
) },
]
}

sub schema_pg {
[
q{
CREATE TEMPORARY table users (
    id serial primary key,
    test varchar(36),
    login varchar(36),
    name varchar(36)
) },
]
}

sub schema_oracle { [
    "CREATE SEQUENCE users_seq",
    "CREATE table users (
        id integer CONSTRAINT users_Key PRIMARY KEY,
        test varchar(36),
        login varchar(36),
        name varchar(36)
    )",
] }

sub cleanup_schema_oracle { [
    "DROP SEQUENCE users_seq",
    "DROP table users", 
] }

package TestApp::User;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column test => type is 'varchar(36)';
    column login => type is 'varchar(36)';
    column name => type is 'varchar(36)';
};
}

sub init_data {
    return (
    [ 'test', 'login', 'name' ],

    [ 'aa', 'a', 'a' ],
    [ 'ab', 'a', 'b' ],
    [ 'ac', 'a', 'c' ],
    [ 'ba', 'b', 'a' ],
    [ 'bb', 'b', 'b' ],
    [ 'bc', 'b', 'c' ],
    [ 'ca', 'c', 'a' ],
    [ 'cb', 'c', 'b' ],
    [ 'cc', 'c', 'c' ],
    );
}

package TestApp::UserCollection;

use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

sub _init {
    my $self = shift;
    $self->table('users');
    return $self->SUPER::_init( @_ );
}

1;
