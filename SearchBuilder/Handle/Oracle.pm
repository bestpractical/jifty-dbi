# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/Oracle.pm,v 1.14 2002/01/28 06:11:37 jesse Exp $

use strict;
package DBIx::SearchBuilder::Handle::Oracle;
use base qw/DBIx::SearchBuilder::Handle/;
use DBD::Oracle qw(:ora_types);
         
use vars qw($VERSION $DBIHandle $DEBUG);


=head1 NAME

  DBIx::SearchBuilder::Handle::Oracle - An oracle specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of DBIx::SearchBuilder::Handle that 
compensates for some of the idiosyncrasies of Oracle.

=head1 METHODS

=cut

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

    my %attribs = @_;
    my ($unique_id, $QueryString);

    if ($attribs{'Id'} || $attribs{'id'}) {
        $unique_id = ($attribs{'Id'} ? $attribs{'Id'} : $attribs{'id'} );
    }
    else {
 
    $QueryString = "SELECT ".$table."_seq.nextval FROM DUAL";
 
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

    $unique_id = $row[0];

    }

    #TODO: don't hardcode this to id pull it from somewhere else
    #call super::Insert with the new column id.

    $attribs{'id'} = $unique_id;
    delete $attribs{'Id'};
    $sth =  $self->SUPER::Insert( $table, %attribs);

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

# {{{ BuildDSN

=head2  BuildDSN PARAMHASH

Takes a bunch of parameters:  

Required: Driver, Database or Host/SID,
Optional: Port and RequireSSL

Builds a DSN suitable for an Oracle DBI connection

=cut

sub BuildDSN {
    my $self = shift;
  my %args = ( Driver => undef,
	       Database => undef,
	       Host => undef,
	       Port => undef,
           SID => undef,
	       RequireSSL => undef,
	       @_);
  
  my $dsn = "dbi:$args{'Driver'}:";

  if (defined $args{'Host'} && $args{'Host'} 
   && defined $args{'SID'}  && $args{'SID'} ) {
      $dsn .= "host=$args{'Host'};sid=$args{'SID'}";
  } else {
      $dsn .= "$args{'Database'}" if (defined $args{'Database'} && $args{'Database'});
  }
  $dsn .= ";port=$args{'Port'}" if (defined $args{'Port'} && $args{'Port'});
  $dsn .= ";requiressl=1" if (defined $args{'RequireSSL'} && $args{'RequireSSL'});

  $self->{'dsn'}= $dsn;
}

# }}}

# {{{ KnowsBLOBs      

=head2 KnowsBLOBs     

Returns 1 if the current database supports inserts of BLOBs automatically.      
Returns undef if the current database must be informed of BLOBs for inserts.    

=cut

sub KnowsBLOBs {     
    my $self = shift;
    return(undef);
}

# }}}

# {{{ BLOBParams 

=head2 BLOBParams FIELD_NAME FIELD_TYPE

Returns a hash ref for the bind_param call to identify BLOB types used by 
the current database for a particular column type.
The current Oracle implementation only supports ORA_CLOB types (112).

=cut

sub BLOBParams { 
    my $self = shift;
    my $field = shift;
    #my $type = shift;
    # Don't assign to key 'value' as it is defined later.
    return ( { ora_field => $field, ora_type => ORA_CLOB,
});    
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


# {{{ BinarySafeBLOBs

=head2 BinarySafeBLOBs

Return undef, as Oracle doesn't support binary-safe CLOBS


=cut

sub BinarySafeBLOBs {
    my $self = shift;
    return(undef);
}

# }}}

1;

__END__

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), DBIx::SearchBuilder

=cut
