#!/usr/bin/perl -w

use strict;

use Test::More;
BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 11;

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

	my $ret = init_schema( 'TestApp::User', $handle );
	isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back");

	my $rec = TestApp::User->new($handle);
	isa_ok($rec, 'Jifty::DBI::Record');

	my $now = time;
	my $dt = DateTime->from_epoch( epoch => $now );
	my($id) = $rec->create( created => $dt );
	ok($id, "Successfuly created ticket");
	ok($rec->load($id), "Loaded the record");
	is($rec->id, $id, "The record has its id");
	isa_ok($rec->created, 'DateTime' );
	is( $rec->created->epoch, $now, "Correct value");

	# undef/NULL
	$rec->set_created;
	is($rec->created, undef, "Set undef value" );

	# from string
	require POSIX;
	$rec->set_created( POSIX::strftime( "%Y-%m-%d %H:%M:%S", gmtime($now) ) );
	isa_ok($rec->created, 'DateTime' );
	is( $rec->created->epoch, $now, "Correct value");
}
}

package TestApp::User;

use base qw/Jifty::DBI::Record/;

sub schema {

    {   
        
        id => { TYPE => 'int(11)' },
        created => { TYPE => 'datetime',
	             input_filters => 'Jifty::DBI::Filter::DateTime',
		   },

    }

}

sub schema_sqlite {

<<EOF;
CREATE TABLE users (
        id integer primary key,
	created datetime
)
EOF

}

sub schema_mysql {

<<EOF;
CREATE TEMPORARY TABLE users (
        id integer auto_increment primary key,
	created datetime
)
EOF

}

sub schema_pg {

<<EOF;
CREATE TEMPORARY TABLE users (
        id serial primary key,
	created timestamp
)
EOF

}

1;

