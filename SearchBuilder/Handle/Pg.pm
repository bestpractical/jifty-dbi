#$Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/Pg.pm,v 1.8 2001/07/27 05:23:29 jesse Exp $
# Copyright 1999-2001 Jesse Vincent <jesse@fsck.com>

package DBIx::SearchBuilder::Handle::Pg;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);


use strict;

# {{{ sub Connect
=head2 Connect

Connect takes a hashref and passes it off to SUPER::Connect;
Forces the timezone to GMT
it returns a database handle.

=cut
  
sub Connect {
    my $self = shift;
    
    $self->SUPER::Connect(@_);
    $self->SimpleQuery("SET TIME ZONE 'GMT'");
    $self->SimpleQuery("SET DATESTYLE TO 'ISO'");
    $self->AutoCommit(1);
    return ($DBIHandle); 
}
# }}}

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

In case of isnert failure, returns a Class::ReturnValue object preloaded
with error info

=cut


sub Insert {
    my $self = shift;
    my $table = shift;
    
    my $sth = $self->SUPER::Insert($table, @_ );
    
    unless ($sth) {
	    return ($sth);
    }

    #Lets get the id of that row we just inserted    
    my $oid = $sth->{'pg_oid_status'};
    my $sql = "SELECT id FROM $table WHERE oid = ?";
    my @row = $self->FetchResult($sql, $oid);
    # TODO: Propagate Class::ReturnValue up here.
    unless ($row[0]) {
	    warn "Can't find $table.id  for OID $oid";
	    return(undef);
    }	
    $self->{'id'} = $row[0];
    
    return ($self->{'id'});
}

# }}}

# {{{ BinarySafeBLOBs

=head2 BinarySafeBLOBs

Return undef, as no current version of postgres supports binary-safe blobs

=cut

sub BinarySafeBLOBs {
    my $self = shift;
    return(undef);
}

# }}}
