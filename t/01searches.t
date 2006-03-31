#!/usr/bin/perl -w


use strict;
use warnings;
use File::Spec;
use Test::More;
BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 59;

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

	my $count_all = init_data( 'TestApp::User', $handle );
	ok( $count_all,  "init users data" );

	my $users_obj = TestApp::UserCollection->new( $handle );
	isa_ok( $users_obj, 'Jifty::DBI::Collection' );
	is( $users_obj->_handle, $handle, "same handle as we used in constructor");

# check that new object returns 0 records in any case
	is( $users_obj->_record_count, 0, '_record_count returns 0 on not limited obj' );
	is( $users_obj->count, 0, 'count returns 0 on not limited obj' );
	is( $users_obj->is_last, undef, 'is_last returns undef on not limited obj after count' );
	is( $users_obj->first, undef, 'first returns undef on not limited obj' );
	is( $users_obj->is_last, undef, 'is_last returns undef on not limited obj after first' );
	is( $users_obj->last, undef, 'last returns undef on not limited obj' );
	is( $users_obj->is_last, undef, 'is_last returns undef on not limited obj after last' );
	$users_obj->goto_first_item;
	is( $users_obj->next, undef, 'next returns undef on not limited obj' );
	is( $users_obj->is_last, undef, 'is_last returns undef on not limited obj after next' );
	# XXX TODO FIXME: may be this methods should be implemented
	# $users_obj->goto_last_item;
	# is( $users_obj->prev, undef, 'prev returns undef on not limited obj' );
	my $items_ref = $users_obj->items_array_ref;
	isa_ok( $items_ref, 'ARRAY', 'items_array_ref always returns array reference' );
	is_deeply( $items_ref, [], 'items_array_ref returns [] on not limited obj' );

# unlimit new object and check
	$users_obj->unlimit;
	is( $users_obj->count, $count_all, 'count returns same number of records as was inserted' );
	isa_ok( $users_obj->first, 'Jifty::DBI::Record', 'first returns record object' );
	isa_ok( $users_obj->last, 'Jifty::DBI::Record', 'last returns record object' );
	$users_obj->goto_first_item;
	isa_ok( $users_obj->next, 'Jifty::DBI::Record', 'next returns record object' );
	$items_ref = $users_obj->items_array_ref;
	isa_ok( $items_ref, 'ARRAY', 'items_array_ref always returns array reference' );
	is( scalar @{$items_ref}, $count_all, 'items_array_ref returns same number of records as was inserted' );
	$users_obj->redo_search;
	$items_ref = $users_obj->items_array_ref;
	isa_ok( $items_ref, 'ARRAY', 'items_array_ref always returns array reference' );
	is( scalar @{$items_ref}, $count_all, 'items_array_ref returns same number of records as was inserted' );

# try to use $users_obj for all tests, after each call to CleanSlate it should look like new obj.
# and test $obj->new syntax
	my $clean_obj = $users_obj->new( $handle );
	isa_ok( $clean_obj, 'Jifty::DBI::Collection' );

# basic limits
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->limit( column => 'login', value => 'obra' );
	is( $users_obj->count, 1, 'found one user with login obra' );
	TODO: {
		local $TODO = 'require discussion';
		is( $users_obj->is_last, undef, 'is_last returns undef before we fetch any record' );
	}
	my $first_rec = $users_obj->first;
	isa_ok( $first_rec, 'Jifty::DBI::Record', 'First returns record object' );
	is( $users_obj->is_last, 1, '1 record in the collection then first rec is last');
	is( $first_rec->login, 'obra', 'login is correct' );
	my $last_rec = $users_obj->last;
	is( $last_rec, $first_rec, 'last returns same object as first' );
	is( $users_obj->is_last, 1, 'is_last always returns 1 after last call');
	$users_obj->goto_first_item;
	my $next_rec = $users_obj->next;
	is( $next_rec, $first_rec, 'next returns same object as first' );
	is( $users_obj->is_last, 1, 'is_last returns 1 after fetch first record with next method');
	is( $users_obj->next, undef, 'only one record in the collection' );
	TODO: {
		local $TODO = 'require discussion';
		is( $users_obj->is_last, undef, 'next returns undef, is_last returns undef too');
	}
	$items_ref = $users_obj->items_array_ref;
	isa_ok( $items_ref, 'ARRAY', 'items_array_ref always returns array reference' );
	is( scalar @{$items_ref}, 1, 'items_array_ref has only 1 record' );

# similar basic limit, but with different operatorS and less first/next/last tests
	# LIKE
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->limit( column => 'name', operator => 'MATCHES', value => 'Glass' );
	is( $users_obj->count, 1, "found one user with 'Glass' in the name" );
	$first_rec = $users_obj->first;
	isa_ok( $first_rec, 'Jifty::DBI::Record', 'First returns record object' );
	is( $first_rec->login, 'glasser', 'login is correct' );

	# STARTSWITH
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->limit( column => 'name', operator => 'STARTSWITH', value => 'Ruslan' );
	is( $users_obj->count, 1, "found one user who name starts with 'Ruslan'" );
	$first_rec = $users_obj->first;
	isa_ok( $first_rec, 'Jifty::DBI::Record', 'First returns record object' );
	is( $first_rec->login, 'cubic', 'login is correct' );

	# ENDSWITH
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->limit( column => 'name', operator => 'ENDSWITH', value => 'Tang' );
	is( $users_obj->count, 1, "found one user who name ends with 'Tang'" );
	$first_rec = $users_obj->first;
	isa_ok( $first_rec, 'Jifty::DBI::Record', 'First returns record object' );
	is( $first_rec->login, 'autrijus', 'login is correct' );

	# IS NULL
	# XXX TODO FIXME: column => undef should be handled as NULL
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->limit( column => 'phone', operator => 'IS', value => 'NULL' );
	is( $users_obj->count, 2, "found 2 users who has unknown phone number" );
	
	# IS NOT NULL
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->limit( column => 'phone', operator => 'IS NOT', value => 'NULL', QOUTEvalue => 0 );
	is( $users_obj->count, $count_all - 2, "found users who has phone number filled" );
	
	# ORDER BY / GROUP BY
	$users_obj->clean_slate;
	is_deeply( $users_obj, $clean_obj, 'after clean_slate looks like new object');
	$users_obj->unlimit;
	$users_obj->group_by(column => 'login');
	$users_obj->order_by(column => 'login', order => 'desc');
	$users_obj->column(column => 'login');
	is( $users_obj->count, $count_all, "group by / order by finds right amount");
	$first_rec = $users_obj->first;
	isa_ok( $first_rec, 'Jifty::DBI::Record', 'First returns record object' );
	is( $first_rec->login, 'obra', 'login is correct' );

	cleanup_schema( 'TestApp', $handle );
	disconnect_handle( $handle );
}} # SKIP, foreach blocks

1;

package TestApp;

sub schema_mysql {
<<EOF;
CREATE TEMPORARY table users (
        id integer AUTO_INCREMENT,
        login varchar(18) NOT NULL,
        name varchar(36),
	phone varchar(18),
  	PRIMARY KEY (id))
EOF

}

sub schema_pg {
<<EOF;
CREATE TEMPORARY table users (
        id serial PRIMARY KEY,
        login varchar(18) NOT NULL,
        name varchar(36),
        phone varchar(18)
)
EOF

}

sub schema_sqlite {

<<EOF;
CREATE table users (
	id integer primary key,
	login varchar(18) NOT NULL,
	name varchar(36),
	phone varchar(18))
EOF

}


1;

package TestApp::User;

use base qw/Jifty::DBI::Record/;

sub _init {
    my $self = shift;
    my $handle = shift;
    $self->table('users');
    $self->_handle($handle);
}

sub init_data {
    return (
	[ 'login',	'name',			'phone' ],
	[ 'cubic',	'Ruslan U. Zakirov',	'+7-903-264-XX-XX' ],
	[ 'obra',	'Jesse Vincent',	undef ],
	[ 'glasser',	'David Glasser',	undef ],
	[ 'autrijus',	'Autrijus Tang',	'+X-XXX-XXX-XX-XX' ],
    );
}

1;

package TestApp::User::Schema;
BEGIN {
    use Jifty::DBI::Schema;

    column login => type is 'varchar(18)';
    column name  => type is 'varchar(36)';
    column phone => type is 'varchar(18)', default is '';
}

1;

package TestApp::UserCollection;

# use TestApp::User;
use base qw/Jifty::DBI::Collection/;

sub _init {
    my $self = shift;
    $self->SUPER::_init( handle => shift );
    $self->table('users');
}

1;

