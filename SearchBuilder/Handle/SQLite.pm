
package DBIx::SearchBuilder::Handle::SQLite;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  DBIx::SearchBuilder::Handle::SQLite -- a mysql specific Handle object

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
    my $table = shift;
    my %args = ( id => undef, @_);
    # We really don't want an empty id
    
    my $sth = $self->SUPER::Insert($table, %args);
    if (!$sth) {
	    return ($sth);
     }

    # If we have set an id, then we want to use that, otherwise, we want to lookup the last _new_ rowid
    $self->{'id'}= $args{'id'} || $self->dbh->func('last_insert_rowid');

    warn "$self no row id returned on row creation" unless ($self->{'id'});
    return( $self->{'id'}); #Add Succeded. return the id
  }

# }}}


=head2 CaseSensitive 

Returns undef, since mysql's searches are not case sensitive by default 

=cut

sub CaseSensitive {
    my $self = shift;
    return(1);
}

sub BinarySafeBLOBs { 
    return undef;
}

# }}}

1;
