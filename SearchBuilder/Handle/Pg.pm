#$Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Handle/Pg.pm,v 1.4 2001/01/25 03:06:31 jesse Exp $
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
    return ($DBIHandle); 
}
# }}}

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

=cut


sub Insert {
    my $self = shift;
    my $table = shift;
    my @keyvals = (@_);
    my $sth = $self->SUPER::Insert($table, @keyvals );
    
    unless ($sth) {
	if ($DEBUG) {
	    die "Error with insert: ". $self->dbh->errstr;
	}
	else {
         return (undef);
     }
    }
    
    #Lets get the id of that row we just inserted
    my $sql = "SELECT id FROM $table WHERE oid = ?";
    my @row = $self->FetchResult($sql, $sth->{'pg_oid_status'});
    $self->{'id'} = $row[0];

    return ($self->{'id'});
}

# }}}
