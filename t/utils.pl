#!/usr/bin/perl -w

use strict;

our @SupportedDrivers = qw(
	Informix
	mysql
	mysqlPP
	ODBC
	Oracle
	Pg
	SQLite
	Sybase
);


sub get_handle
{
	my $type = shift;
	my $class = 'DBIx::SearchBuilder::Handle::'. $type;
	eval "require $class";
	die $@ if $@;
	my $handle;
	{
#		no strict 'refs';
		$handle = $class->new( @_ );
	}
	return $handle;
}

1;
