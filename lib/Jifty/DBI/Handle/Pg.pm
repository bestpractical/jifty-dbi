package Jifty::DBI::Handle::Pg;
use strict;

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use base qw(Jifty::DBI::Handle);

use strict;

=head1 NAME

  Jifty::DBI::Handle::Pg - A Postgres specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of L<Jifty::DBI::Handle> that
compensates for some of the idiosyncrasies of Postgres.

=head1 METHODS

=cut

=head2 connect

connect takes a hashref and passes it off to SUPER::connect; Forces
the timezone to GMT, returns a database handle.

=cut

sub connect {
    my $self = shift;

    $self->SUPER::connect(@_);
    $self->simple_query("SET TIME ZONE 'GMT'");
    $self->simple_query("SET DATESTYLE TO 'ISO'");
    $self->auto_commit(1);
    return ($DBIHandle);
}

=head2 insert

Takes a table name as the first argument and assumes that the rest of
the arguments are an array of key-value pairs to be inserted.

In case of insert failure, returns a L<Class::ReturnValue> object
preloaded with error info

=cut

sub insert {
    my $self  = shift;
    my $table = shift;

    my $sth = $self->SUPER::insert( $table, @_ );

    unless ($sth) {
        return ($sth);
    }

    #Lets get the id of that row we just inserted
    my $oid = $sth->{'pg_oid_status'};
    my $sql = "SELECT id FROM $table WHERE oid = ?";
    my @row = $self->fetch_result( $sql, $oid );

    # TODO: Propagate Class::ReturnValue up here.
    unless ( $row[0] ) {
        print STDERR "Can't find $table.id  for OID $oid";
        return (undef);
    }
    $self->{'id'} = $row[0];

    return ( $self->{'id'} );
}

=head2 binary_safe_blobs

Return undef, as no current version of postgres supports binary-safe
blobs

=cut

sub binary_safe_blobs {
    my $self = shift;
    return (undef);
}

=head2 apply_limits STATEMENTREF ROWS_PER_PAGE FIRST_ROW

takes an SQL SELECT statement and massages it to return ROWS_PER_PAGE
starting with FIRST_ROW;

=cut

sub apply_limits {
    my $self         = shift;
    my $statementref = shift;
    my $per_page     = shift;
    my $first        = shift;

    my $limit_clause = '';

    if ($per_page) {
        $limit_clause = " LIMIT ";
        $limit_clause .= $per_page;
        if ( $first && $first != 0 ) {
            $limit_clause .= " OFFSET $first";
        }
    }

    $$statementref .= $limit_clause;

}

=head2 _make_clause_case_insensitive column operator VALUE

Takes a column, operator and value. performs the magic necessary to make
your database treat this clause as case insensitive.

Returns a column operator value triple.

=cut

sub _make_clause_case_insensitive {
    my $self     = shift;
    my $column    = shift;
    my $operator = shift;
    my $value    = shift;

    if ( $value =~ /^['"]?\d+['"]?$/ )
    {    # we don't need to downcase numeric values
        return ( $column, $operator, $value );
    }

    if ( $operator =~ /LIKE/i ) {
        $operator =~ s/LIKE/ILIKE/ig;
        return ( $column, $operator, $value );
    }
    elsif ( $operator =~ /=/ ) {
        return ( "LOWER($column)", $operator, $value, "LOWER(?)" );
    }
    else {
        $self->SUPER::_make_clause_case_insensitive( $column, $operator,
            $value );
    }
}

1;

__END__

=head1 SEE ALSO

L<Jifty::DBI>, L<Jifty::DBI::Handle>, L<DBD::Pg>

=cut

