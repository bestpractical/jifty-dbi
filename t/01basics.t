#!/usr/bin/perl -w

use strict;

use Test::More;
BEGIN { require "t/utils.pl" }
our (@SupportedDrivers);

my $total = scalar(@SupportedDrivers) * 4;
plan tests => $total;

foreach my $d ( @SupportedDrivers ) {
SKIP: {
	eval "require DBD::$d";
	if( $@ ) {
		skip "DBD::$d is not installed", 4;
	}
	use_ok('DBIx::SearchBuilder::Handle::'. $d);
	my $handle = get_handle( $d );
	isa_ok($handle, 'DBIx::SearchBuilder::Handle');
	isa_ok($handle, 'DBIx::SearchBuilder::Handle::'. $d);
	can_ok($handle, 'dbh');
}
}


1;
