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

# }}}

# {{{ ApplyLimits

=head2 ApplyLimits STATEMENTREF ROWS_PER_PAGE FIRST_ROW

takes an SQL SELECT statement and massages it to return ROWS_PER_PAGE starting with FIRST_ROW;


=cut

sub ApplyLimits {
    my $self = shift;
    my $statementref = shift;
    my $per_page = shift;
    my $first = shift;

    # Transform an SQL query from:
    #
    # SELECT main.* 
    #   FROM Tickets main   
    #  WHERE ((main.EffectiveId = main.id)) 
    #    AND ((main.Type = 'ticket')) 
    #    AND ( ( (main.Status = 'new')OR(main.Status = 'open') ) 
    #    AND ( (main.Queue = '1') ) )  
    #
    # to: 
    #
    # SELECT * FROM (
    #     SELECT limitquery.*,rownum limitrownum FROM (
    #             SELECT main.* 
    #               FROM Tickets main   
    #              WHERE ((main.EffectiveId = main.id)) 
    #                AND ((main.Type = 'ticket')) 
    #                AND ( ( (main.Status = 'new')OR(main.Status = 'open') ) 
    #                AND ( (main.Queue = '1') ) )  
    #     ) limitquery WHERE rownum <= 50
    # ) WHERE limitrownum >= 1
    #

    if ($per_page) {
        # Oracle orders from 1 not zero
        $first++; 
        # Make current query a sub select
        $$statementref = "SELECT * FROM ( SELECT limitquery.*,rownum limitrownum FROM ( $$statementref ) limitquery WHERE rownum <= " . ($first + $per_page - 1) . " ) WHERE limitrownum >= " . $first;
    }
}

# }}}

# {{{ DistinctQuery

=head2 DistinctQuery STATEMENTREF

takes an incomplete SQL SELECT statement and massages it to return a DISTINCT result set.


=cut

sub DistinctQuery {
    my $self = shift;
    my $statementref = shift;
    my $table = shift;

    # Wrapper select query in a subselect as Oracle doesn't allow
    # DISTINCT against CLOB/BLOB column types.
    $$statementref = "SELECT main.* FROM ( SELECT DISTINCT main.id FROM $$statementref ) distinctquery, $table main WHERE (main.id = distinctquery.id) ";

}

# }}}

