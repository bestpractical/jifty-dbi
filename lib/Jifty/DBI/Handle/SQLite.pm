
package Jifty::DBI::Handle::SQLite;
use Jifty::DBI::Handle;
@ISA = qw(Jifty::DBI::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  Jifty::DBI::Handle::SQLite -- A SQLite specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of Jifty::DBI::Handle that 
compensates for some of the idiosyncrasies of SQLite.

=head1 METHODS

=head2 database_version

Returns the version of the SQLite library which is used, e.g., "2.8.0".
SQLite can only return short variant.

=cut

sub database_version {
    my $self = shift;
    return '' unless $self->dbh;
    return $self->dbh->{sqlite_version} || '';
}

=head2 insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

If the insert succeeds, returns the id of the insert, otherwise, returns
a Class::ReturnValue object with the error reported.

=cut

sub insert {
    my $self  = shift;
    my $table = shift;
    my %args  = ( id => undef, @_ );

    # We really don't want an empty id

    my $sth = $self->SUPER::insert( $table, %args );
    return unless $sth;

# If we have set an id, then we want to use that, otherwise, we want to lookup the last _new_ rowid
    $self->{'id'} = $args{'id'} || $self->dbh->func('last_insert_rowid');

    warn "$self no row id returned on row creation" unless ( $self->{'id'} );
    return ( $self->{'id'} );    #Add Succeded. return the id
}

=head2 case_sensitive 

Returns 1, since SQLite's searches are case sensitive by default.
Note, however, SQLite's C<like> operator is case I<in>sensitive.

=cut

sub case_sensitive {
    my $self = shift;
    return (1);
}

=head2 distinct_count STATEMENTREF

takes an incomplete SQL SELECT statement and massages it to return a DISTINCT result count


=cut

sub distinct_count {
    my $self         = shift;
    my $statementref = shift;

    # Wrapper select query in a subselect as Oracle doesn't allow
    # DISTINCT against CLOB/BLOB column types.
    $$statementref
        = "SELECT count(*) FROM (SELECT DISTINCT main.id FROM $$statementref )";

}

1;

__END__

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), Jifty::DBI

=cut
