# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/ODBC.pm,v 1.8 2001/10/12 05:27:05 jesse Exp $

package DBIx::SearchBuilder::Handle::ODBC;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

sub CaseSensitive {
    my $self = shift;
    return (undef);
}

sub BuildDSN {
    my $self = shift;
    my %args = (
	Driver     => undef,
	Database   => undef,
	Host       => undef,
	Port       => undef,
	@_
    );

    my $dsn = "dbi:$args{'Driver'}:$args{'Database'}";
    $dsn .= ";host=$args{'Host'}" if (defined $args{'Host'} && $args{'Host'});
    $dsn .= ";port=$args{'Port'}" if (defined $args{'Port'} && $args{'Port'});

    $self->{'dsn'} = $dsn;
}

sub ApplyLimits {
    my $self         = shift;
    my $statementref = shift;
    my $per_page     = shift or return;
    my $first        = shift;

    my $limit_clause = " TOP $per_page";
    $limit_clause .= " OFFSET $first" if $first;
    $$statementref =~ s/SELECT\b/SELECT $limit_clause/;
}

sub DistinctQuery {
    my $self         = shift;
    my $statementref = shift;

    $$statementref = "SELECT main.* FROM $$statementref";
}

sub Encoding {
}

