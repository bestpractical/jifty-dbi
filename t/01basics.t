#!/usr/bin/perl -w

use strict;

use Test::More;
BEGIN { require "t/utils.pl" }
our (@AvailableDrivers);

use constant TESTS_PER_DRIVER => 4;

my $total = scalar(@AvailableDrivers) * TESTS_PER_DRIVER;
plan tests => $total;

foreach my $d ( @AvailableDrivers ) {
SKIP: {
	use_ok('Jifty::DBI::Handle::'. $d);
	my $handle = get_handle( $d );
	isa_ok($handle, 'Jifty::DBI::Handle');
	isa_ok($handle, 'Jifty::DBI::Handle::'. $d);
	can_ok($handle, 'dbh');
}
}


1;
