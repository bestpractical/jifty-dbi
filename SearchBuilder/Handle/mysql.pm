# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/mysql.pm,v 1.8 2001/10/12 05:27:05 jesse Exp $

package DBIx::SearchBuilder::Handle::mysql;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  DBIx::SearchBuilder::Handle::mysql - A mysql specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of DBIx::SearchBuilder::Handle that 
compensates for some of the idiosyncrasies of MySQL.

=head1 METHODS

=cut


=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments are an array of key-value pairs to be inserted.

If the insert succeeds, returns the id of the insert, otherwise, returns
a Class::ReturnValue object with the error reported.

=cut

sub Insert  {
    my $self = shift;

    my $sth = $self->SUPER::Insert(@_);
    if (!$sth) {
	    return ($sth);
     }

    $self->{'id'}=$self->dbh->{'mysql_insertid'};
 
    # Yay. we get to work around mysql_insertid being null some of the time :/
    unless ($self->{'id'}) {
	$self->{'id'} =  $self->FetchResult('SELECT LAST_INSERT_ID()');
    }
    warn "$self no row id returned on row creation" unless ($self->{'id'});
    
    return( $self->{'id'}); #Add Succeded. return the id
  }



=head2 DatabaseVersion

Returns the mysql version, trimming off any -foo identifier

=cut

sub DatabaseVersion {
    my $self = shift;
    my $v = $self->SUPER::DatabaseVersion();

   $v =~ s/\-.*$//;
   return ($v);
}

=head2 CaseSensitive 

Returns undef, since mysql's searches are not case sensitive by default 

=cut

sub CaseSensitive {
    my $self = shift;
    return(undef);
}


1;

__END__

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

DBIx::SearchBuilder, DBIx::SearchBuilder::Handle

=cut

