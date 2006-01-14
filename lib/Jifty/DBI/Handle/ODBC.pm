package Jifty::DBI::Handle::ODBC;
use Jifty::DBI::Handle;
@ISA = qw(Jifty::DBI::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  Jifty::DBI::Handle::ODBC - An ODBC specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of L<Jifty::DBI::Handle> that
compensates for some of the idiosyncrasies of ODBC.

=head1 METHODS

=cut

=head2 CaseSensitive

Returns a false value.

=cut

sub case_sensitive {
    my $self = shift;
    return (undef);
}

=head2 BuildDSN

=cut

sub build_dsn {
    my $self = shift;
    my %args = (
        Driver   => undef,
        Database => undef,
        Host     => undef,
        Port     => undef,
        @_
    );

    my $dsn = "dbi:$args{'Driver'}:$args{'Database'}";
    $dsn .= ";host=$args{'Host'}"
        if ( defined $args{'Host'} && $args{'Host'} );
    $dsn .= ";port=$args{'Port'}"
        if ( defined $args{'Port'} && $args{'Port'} );

    $self->{'dsn'} = $dsn;
}

=head2 ApplyLimits

=cut

sub apply_limits {
    my $self         = shift;
    my $statementref = shift;
    my $per_page     = shift or return;
    my $first        = shift;

    my $limit_clause = " TOP $per_page";
    $limit_clause .= " OFFSET $first" if $first;
    $$statementref =~ s/SELECT\b/SELECT $limit_clause/;
}

=head2 DistinctQuery

=cut

sub distinct_query {
    my $self         = shift;
    my $statementref = shift;

    my $sb = shift;

    $$statementref = "SELECT main.* FROM $$statementref";
    $$statementref .= $sb->_group_clause;
    $$statementref .= $sb->_order_clause;
}

sub encoding {
}

1;

__END__

=head1 AUTHOR

Autrijus Tang

=head1 SEE ALSO

L<Jifty::DBI>, L<Jifty::DBI::Handle>, L<DBD::ODBC>

=cut
