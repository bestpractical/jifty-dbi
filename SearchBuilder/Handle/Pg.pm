package DBIx::SearchBuilder::Handle::Pg;
use DBIx::SearchBuilder::Handle;
@ISA = qw( DBIx::SearchBuilder::Handle );


=head2 Connect

Connect takes a hashref and passes it off to SUPER::Connect;
Forces the timezone to GMT
it returns a database handle.

=cut
  
sub Connect {
    my $self = shift;
    
    $self->SUPER::Connect(@_);
    $self->SimpleQuery("SET TIME ZONE 'GMT'");
    return ($Handle); 
}
# }}}


sub Insert {
  my($self, $table) = (shift, shift);
  #my $sth = $self->SUPER::Insert($table, @_, 'id', undef);
  my $sth = $self->SUPER::Insert($table, @_ );

  unless ($sth) {
     if ($main::debug) {
        die "Error with $QueryString: ". $self->dbh->errstr;
    }
     else {
         return (undef);
     }
   }

   my $oid = $sth->{'pg_oid_status'};
   #warn "oid $oid\n";

   my $sql = "SELECT id FROM $table WHERE oid = $oid";
   my $osth = $self->dbh->prepare($sql);
   $osth->execute or die $osth->errstr;
   my @row = $osth->fetchrow or die "can't find $table.id for oid $oid";

   #warn "$row ". $row->{id}. "\n";
   return ($row[0]);
}

