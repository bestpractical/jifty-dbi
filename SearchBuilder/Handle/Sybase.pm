# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/mysql.pm,v 1.8 2001/10/12 05:27:05 jesse Exp $

package DBIx::SearchBuilder::Handle::Sybase;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  DBIx::SearchBuilder::Handle::mysql -- a mysql specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), DBIx::SearchBuilder

=cut

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.


If the insert succeeds, returns the id of the insert, otherwise, returns
a Class::ReturnValue object with the error reploaded.

=cut

sub Insert  {
    my $self = shift;

    my $sth = $self->SUPER::Insert(@_);
    if (!$sth) {
	    return ($sth);
     }
    my $sql = 'SELECT @@identity';
    my @row = $self->FetchResult($sql);
    # TODO: Propagate Class::ReturnValue up here.
    unless ($row[0]) {
            return(undef);
    }   
    $self->{'id'} = $row[0];
    
    return ($self->{'id'});
}



# }}}


=head2 DatabaseVersion

return the mysql version, trimming off any -foo identifier

=cut

sub DatabaseVersion {
    my $self = shift;
    my $v = $self->SUPER::DatabaseVersion();

   $v =~ s/\-(.*)$//;
   return ($v);

}

=head2 CaseSensitive 

Returns undef, since mysql's searches are not case sensitive by default 

=cut

sub CaseSensitive {
    my $self = shift;
    return(undef);
}


# }}}


