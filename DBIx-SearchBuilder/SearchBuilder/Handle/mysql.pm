# $Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Handle/mysql.pm,v 1.1 2000/09/11 16:53:00 jesse Exp $

package DBIx::SearchBuilder::Handle::mysql;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);



sub new  {
      my $proto = shift;
      my $class = ref($proto) || $proto;
      my $self  = {};
      bless ($self, $class);
      return ($self);
}

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

=cut

sub Insert  {
    my $self = shift;
    my $table = shift;
    my @keyvalpairs = (@_);

    my ($cols, $vals);
    
    while (my $key = shift @keyvalpairs) {
      my $value = shift @keyvalpairs;
    
      $cols .= $key . ", ";
      if (defined ($value)) {
	  $value = $self->safe_quote($value)
	      unless ($key=~/^(Created|LastUpdated)$/ && $value=~/^now\(\)$/i);
	  $vals .= "$value, ";
      }
      else {
	$vals .= "NULL, ";
      }
    }	
    
    $cols =~ s/, $//;
    $vals =~ s/, $//;
    #TODO Check to make sure the key's not already listed.
    #TODO update internal data structure
    my $QueryString = "INSERT INTO ".$table." ($cols) VALUES ($vals)";

    my $sth = $self->SimpleQuery($QueryString);
    if (!$sth) {
       if ($main::debug) {
	die "Error with $QueryString";
      }
       else {
	 return (0);
       }
     }

    $self->{'id'}=$sth->{'mysql_insertid'};
    return( $self->{'id'}); #Add Succeded. return the id
  }

# }}}



=head1 NAME

  DBIx::SearchBuilder::Handle::mysql -- a mysql specific Handle object

=head1 SYNOPSIS


  =head1 DESCRIPTION

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), DBIx::SearchBuilder

=cut
