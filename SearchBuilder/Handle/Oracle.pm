# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/Oracle.pm,v 1.14 2002/01/28 06:11:37 jesse Exp $

package DBIx::SearchBuilder::Handle::Oracle;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);

use strict;

=head1 NAME

  DBIx::SearchBuilder::Handle::Oracle -- an oracle specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), DBIx::SearchBuilder

=cut


sub new  {
      my $proto = shift;
      my $class = ref($proto) || $proto;
      my $self  = {};
      bless ($self, $class);
      return ($self);
}


# {{{ sub Connect 

=head2 Connect PARAMHASH: Driver, Database, Host, User, Password

Takes a paramhash and connects to your DBI datasource. 


=cut

sub Connect  {
  my $self = shift;
  
  my %args = ( Driver => undef,
	       Database => undef,
	       User => undef,
	       Password => undef, 
           SID => undef,
           Host => undef,
	       @_);
  
    $self->SUPER::Connect(%args);
   
    
    $self->dbh->{LongTruncOk}=1;
    $self->dbh->{LongReadLen}=8000;
    
    $self->SimpleQuery("ALTER SESSION set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
    
    return ($DBIHandle); 
}
# }}}

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

=cut

sub Insert  {
	my $self = shift;
	my $table = shift;
    my ($sth);



  # Oracle Hack to replace non-supported mysql_rowid call
 
    my $QueryString = "SELECT ".$table."_seq.nextval FROM DUAL";
 
    $sth = $self->SimpleQuery($QueryString);
    if (!$sth) {
       if ($main::debug) {
    	die "Error with $QueryString";
      }
       else {
	 return (undef);
       }
     }

     #needs error checking
    my @row = $sth->fetchrow_array;

    my $unique_id = $row[0];

    #TODO: don't hardcode this to id pull it from somewhere else
    #call super::Insert with the new column id.

   $sth =  $self->SUPER::Insert( $table, 'id', $unique_id, @_);

   unless ($sth) {
     if ($main::debug) {
        die "Error with $QueryString: ". $self->dbh->errstr;
    }
     else {
         return (undef);
     }
   }

    $self->{'id'} = $unique_id;
    return( $self->{'id'}); #Add Succeded. return the id
  }



