# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/Sybase.pm,v 1.8 2001/10/12 05:27:05 jesse Exp $

package Jifty::DBI::Handle::Sybase;
use Jifty::DBI::Handle;
@ISA = qw(Jifty::DBI::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  Jifty::DBI::Handle::Sybase -- a Sybase specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of Jifty::DBI::Handle that 
compensates for some of the idiosyncrasies of Sybase.

=head1 METHODS

=cut


=head2 insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

If the insert succeeds, returns the id of the insert, otherwise, returns
a Class::ReturnValue object with the error reported.

=cut

sub insert {
    my $self  = shift;

    my $table = shift;
    my %pairs = @_;
    my $sth   = $self->SUPER::insert( $table, %pairs );
    if ( !$sth ) {
        return ($sth);
    }
    
    # Can't select identity column if we're inserting the id by hand.
    unless ($pairs{'id'}) {
        my @row = $self->fetch_result('SELECT @@identity');

        # TODO: Propagate Class::ReturnValue up here.
        unless ( $row[0] ) {
            return (undef);
        }
        $self->{'id'} = $row[0];
    }
    return ( $self->{'id'} );
}





=head2 database_version

return the database version, trimming off any -foo identifier

=cut

sub database_version {
    my $self = shift;
    my $v = $self->SUPER::database_version();

   $v =~ s/\-(.*)$//;
   return ($v);

}

=head2 case_sensitive 

Returns undef, since Sybase's searches are not case sensitive by default 

=cut

sub case_sensitive {
    my $self = shift;
    return(1);
}




sub apply_limits {
    my $self = shift;
    my $statementref = shift;
    my $per_page = shift;
    my $first = shift;

}


=head2 distinct_query STATEMENTREFtakes an incomplete SQL SELECT statement and massages it to return a DISTINCT result set.


=cut

sub distinct_query {
    my $self = shift;
    my $statementref = shift;
    my $table = shift;

    # Wrapper select query in a subselect as Oracle doesn't allow
    # DISTINCT against CLOB/BLOB column types.
    $$statementref = "SELECT main.* FROM ( SELECT DISTINCT main.id FROM $$statementref ) distinctquery, $table main WHERE (main.id = distinctquery.id) ";

}


=head2 binary_safe_blobs

Return undef, as Oracle doesn't support binary-safe CLOBS


=cut

sub binary_safe_blobs {
    my $self = shift;
    return(undef);
}



1;

__END__

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

Jifty::DBI, Jifty::DBI::Handle

=cut
