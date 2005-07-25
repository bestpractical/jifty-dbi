#!/usr/bin/perl -w


use strict;
use warnings;
use File::Spec;
use Test::More;
use Jifty::DBI::Handle;

BEGIN { require "t/utils.pl" }
our (@AvailableDrivers);

use constant TESTS_PER_DRIVER => 4;

my $total = scalar(@AvailableDrivers) * TESTS_PER_DRIVER;
plan tests => $total;

foreach my $d ( @AvailableDrivers ) {
SKIP: {
	unless( should_test( $d ) ) {
		skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
	}

	my $handle = Jifty::DBI::Handle->new;
	ok($handle, "Made a generic handle");
	
	is(ref $handle, 'Jifty::DBI::Handle', "It's really generic");
	
	connect_handle_with_driver( $handle, $d );
	isa_ok($handle->dbh, 'DBI::db');
	
	isa_ok($handle, "Jifty::DBI::Handle::$d", "Specialized Handle")
}} # SKIP, foreach blocks

1;
