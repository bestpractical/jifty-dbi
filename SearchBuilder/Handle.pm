# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle.pm,v 1.21 2002/01/28 06:11:37 jesse Exp $
package DBIx::SearchBuilder::Handle;
use Carp;
use DBI;
use strict;
use Class::ReturnValue;
use vars qw($VERSION @ISA %DBIHandle $PrevHandle $DEBUG $TRANSDEPTH);

$TRANSDEPTH = 0;

$VERSION = '$Version$';


# {{{ Top POD

=head1 NAME

DBIx::SearchBuilder::Handle - Perl extension which is a generic DBI handle

=head1 SYNOPSIS

  use DBIx::SearchBuilder::Handle;

 my $Handle = DBIx::SearchBuilder::Handle->new();
 $Handle->Connect( Driver => 'mysql',
		   Database => 'dbname',
		   Host => 'hostname',
		   User => 'dbuser',
		   Password => 'dbpassword');
 
 

=head1 DESCRIPTION

Jesse's a slacker.

Blah blah blah.

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com
 
=cut

# }}}

# {{{ sub new 

=head2 new

Generic constructor

=cut

sub new  {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless ($self, $class);
    return $self;
}

# }}}

# {{{ sub Insert
=head2 Insert $TABLE_NAME @KEY_VALUE_PAIRS

Takes a table name and a set of key-value pairs in an array. splits the key value pairs, constructs an INSERT statement and performs the insert. Returns the row_id of this row.

=cut

sub Insert {
  my($self, $table, @pairs) = @_;
  my(@cols, @vals, @bind);

#  my %seen; #only the *first* value is used - allows drivers to specify default
  while ( my $key = shift @pairs ) {
    my $value = shift @pairs;
    #    next if $seen{$key}++;
    push @cols, $key;
    push @vals, '?';
    push @bind, $value;  
  }

  my $QueryString =
    "INSERT INTO $table (". join(", ", @cols). ") VALUES ".
    "(". join(", ", @vals). ")";

    my $sth =  $self->SimpleQuery($QueryString, @bind);
    return ($sth);
  }
# }}}

# {{{ sub Connect 

=head2 Connect PARAMHASH: Driver, Database, Host, User, Password

Takes a paramhash and connects to your DBI datasource. 

You should _always_ set

     DisconnectHandleOnDestroy => 1 

unless you have a legacy app like RT2 or RT 3.0.{0,1,2} that depends on the broken behaviour.
=cut

sub Connect  {
  my $self = shift;
  
  my %args = ( Driver => undef,
	       Database => undef,
	       Host => undef,
           SID => undef,
	       Port => undef,
	       User => undef,
	       Password => undef,
	       RequireSSL => undef,
           DisconnectHandleOnDestroy => undef,
	       @_);

    my $dsn = $self->DSN;

    # Setting this actually breaks old RT versions in subtle ways. So we need to explicitly call it

    $self->{'DisconnectHandleOnDestroy'} = $args{'DisconnectHandleOnDestroy'};

  $self->BuildDSN(%args);

    # Only connect if we're not connected to this source already
   if ((! $self->dbh ) || (!$self->dbh->ping) || ($self->DSN ne $dsn) ) { 
     my $handle = DBI->connect($self->DSN, $args{'User'}, $args{'Password'}) || croak "Connect Failed $DBI::errstr\n" ;
 
  #databases do case conversion on the name of columns returned. 
  #actually, some databases just ignore case. this smashes it to something consistent 
  $handle->{FetchHashKeyName} ='NAME_lc';

  #Set the handle 
  $self->dbh($handle);
  return (1); 
    }

    return(undef);

}
# }}}

# {{{ BuildDSN

=head2 BuildDSN PARAMHASH

Takes a bunch of parameters:  

Required: Driver, Database,
Optional: Host, Port and RequireSSL

Builds a DSN suitable for a DBI connection

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
  
  
  my $dsn = "dbi:$args{'Driver'}:dbname=$args{'Database'}";
  $dsn .= ";sid=$args{'SID'}" if ( defined $args{'SID'} && $args{'SID'});
  $dsn .= ";host=$args{'Host'}" if (defined$args{'Host'} && $args{'Host'});
  $dsn .= ";port=$args{'Port'}" if (defined $args{'Port'} && $args{'Port'});
  $dsn .= ";requiressl=1" if (defined $args{'RequireSSL'} && $args{'RequireSSL'});

  $self->{'dsn'}= $dsn;
}

# }}}

# {{{ DSN

=head2 DSN

    Returns the DSN for this database connection.

=cut
sub DSN {
    my $self = shift;
    return($self->{'dsn'});
}

# }}}

# {{{ RaiseError

=head2 RaiseError [MODE]

Turns on the Database Handle's RaiseError attribute.

=cut

sub RaiseError {
    my $self = shift;

    my $mode = 1; 
    $mode = shift if (@_);

    $self->dbh->{RaiseError}=$mode;
}


# }}}

# {{{ PrintError

=head2 PrintError [MODE]

Turns on the Database Handle's PrintError attribute.

=cut

sub PrintError {
    my $self = shift;

    my $mode = 1; 
    $mode = shift if (@_);

    $self->dbh->{PrintError}=$mode;
}


# }}}

# {{{ AutoCommit

=head2 AutoCommit [MODE]

Turns on the Database Handle's AutoCommit attribute.

=cut

sub AutoCommit {
    my $self = shift;

    my $mode = 1; 
    $mode = shift if (@_);

    $self->dbh->{AutoCommit}=$mode;
}


# }}}

# {{{ sub Disconnect 

=head2 Disconnect

Disconnect from your DBI datasource

=cut

sub Disconnect  {
  my $self = shift;
  if ($self->dbh) {
      return ($self->dbh->disconnect());
  } else {
      return;
  }
}

# }}}

# {{{ sub Handle / dbh 

=head2 dbh [HANDLE]

Return the current DBI handle. If we're handed a parameter, make the database handle that.

=cut

# allow use of Handle as a synonym for DBH
*Handle=\&dbh;

sub dbh {
  my $self=shift;
  
  #If we are setting the database handle, set it.
  $DBIHandle{$self} = $PrevHandle = shift if (@_);

  return($DBIHandle{$self} ||= $PrevHandle);
}

# }}}

# {{{ sub UpdateRecordValue 

=head2 UpdateRecordValue 

Takes a hash with fields: Table, Column, Value PrimaryKeys, and 
IsSQLFunction.  Table, and Column should be obvious, Value is where you 
set the new value you want the column to have. The primary_keys field should 
be the lvalue of DBIx::SearchBuilder::Record::PrimaryKeys().  Finally 
IsSQLFunction is set when the Value is a SQL function.  For example, you 
might have ('Value'=>'PASSWORD(string)'), by setting IsSQLFunction that 
string will be inserted into the query directly rather then as a binding. 

=cut

## Please see file perltidy.ERR
sub UpdateRecordValue {
    my $self = shift;
    my %args = ( Table         => undef,
                 Column        => undef,
                 IsSQLFunction => undef,
                 PrimaryKeys   => undef,
                 @_ );

    my @bind  = ();
    my $query = 'UPDATE ' . $args{'Table'} . ' ';
     $query .= 'SET '    . $args{'Column'} . '=';

  ## Look and see if the field is being updated via a SQL function. 
  if ($args{'IsSQLFunction'}) {
     $query .= $args{'Value'} . ' ';
  }
  else {
     $query .= '? ';
     push (@bind, $args{'Value'});
  }

  ## Constructs the where clause.
  my $where  = 'WHERE ';
  foreach my $key (keys %{$args{'PrimaryKeys'}}) {
     $where .= $key . "=?" . " AND ";
     push (@bind, $args{'PrimaryKeys'}{$key});
  }
     $where =~ s/AND\s$//;
  
  my $query_str = $query . $where;
  return ($self->SimpleQuery($query_str, @bind));
}




=head2 UpdateTableValue TABLE COLUMN NEW_VALUE RECORD_ID IS_SQL

Update column COLUMN of table TABLE where the record id = RECORD_ID.  if IS_SQL is set,
don\'t quote the NEW_VALUE

=cut

sub UpdateTableValue  {
    my $self = shift;

    ## This is just a wrapper to UpdateRecordValue().     
    my %args = (); 
    $args{'Table'}  = shift;
    $args{'Column'} = shift;
    $args{'Value'}  = shift;
    $args{'PrimaryKeys'}   = shift; 
    $args{'IsSQLFunction'} = shift;

    return $self->UpdateRecordValue(%args)
}
# }}}

# {{{ sub SimpleQuery

=head2 SimpleQuery QUERY_STRING, [ BIND_VALUE, ... ]

Execute the SQL string specified in QUERY_STRING

=cut

sub SimpleQuery  {
    my $self = shift;
    my $QueryString = shift;
    my @bind_values = (@_);

    my $sth = $self->dbh->prepare($QueryString);
    unless ($sth) {
	if ($DEBUG) {
	    die "$self couldn't prepare the query '$QueryString'" . 
	      $self->dbh->errstr . "\n";
	}
	else {
	    warn "$self couldn't prepare the query '$QueryString'" . 
	      $self->dbh->errstr . "\n";
        my $ret = Class::ReturnValue->new();
        $ret->as_error( errno => '-1',
                            message => "Couldn't prepare the query '$QueryString'.". $self->dbh->errstr,
                            do_backtrace => undef);
	    return ($ret->return_value);
	}
    }

    # Check @bind_values for HASH refs 
    for (my $bind_idx = 0; $bind_idx < scalar @bind_values; $bind_idx++) {
        if (ref($bind_values[$bind_idx]) eq "HASH") {
            my $bhash = $bind_values[$bind_idx];
            $bind_values[$bind_idx] = $bhash->{'value'};
            delete $bhash->{'value'};
            $sth->bind_param($bind_idx+1, undef, $bhash );
        }
    }
    $self->Log($QueryString. " (".join(',',@bind_values).")") if ($DEBUG);
    unless ( $sth->execute(@bind_values) ) {
        if ($DEBUG) {
            die "$self couldn't execute the query '$QueryString'"
              . $self->dbh->errstr . "\n";

        }
        else {
            warn "$self couldn't execute the query '$QueryString'";

              my $ret = Class::ReturnValue->new();
            $ret->as_error(
                         errno   => '-1',
                         message => "Couldn't execute the query '$QueryString'"
                           . $self->dbh->errstr,
                         do_backtrace => undef );
            return ($ret->return_value);
        }

    }
    return ($sth);
    
    
  }

# }}}

# {{{ sub FetchResult

=head2 FetchResult QUERY, [ BIND_VALUE, ... ]

Takes a SELECT query as a string, along with an array of BIND_VALUEs
If the select succeeds, returns the first row as an array.
Otherwise, returns a Class::ResturnValue object with the failure loaded
up.

=cut 

sub FetchResult {
  my $self = shift;
  my $query = shift;
  my @bind_values = @_;
  my $sth = $self->SimpleQuery($query, @bind_values);
  if ($sth) {
    return ($sth->fetchrow);
  }
  else {
   return($sth);
  }
}
# }}}

# {{{ BinarySafeBLOBs

=head2 BinarySafeBLOBs

Returns 1 if the current database supports BLOBs with embedded nulls.
Returns undef if the current database doesn't support BLOBs with embedded nulls

=cut

sub BinarySafeBLOBs {
    my $self = shift;
    return(1);
}

# }}}

# {{{ KnowsBLOBs

=head2 KnowsBLOBs

Returns 1 if the current database supports inserts of BLOBs automatically.
Returns undef if the current database must be informed of BLOBs for inserts.

=cut

sub KnowsBLOBs {
    my $self = shift;
    return(1);
}

# }}}

# {{{ BLOBParams

=head2 BLOBParams FIELD_NAME FIELD_TYPE

Returns a hash ref for the bind_param call to identify BLOB types used by 
the current database for a particular column type.                 

=cut

sub BLOBParams {
    my $self = shift;
    # Don't assign to key 'value' as it is defined later. 
    return ( {} );
}

# }}}

# {{{ DatabaseVersion

=head2 DatabaseVersion

Returns the database's version. The base implementation uses a "SELECT VERSION"

=cut

sub DatabaseVersion {
    my $self = shift;

    unless ($self->{'database_version'}) {
        my $statement  = "SELECT VERSION()";
        my $sth = $self->SimpleQuery($statement);
        my @vals = $sth->fetchrow();
        $self->{'database_version'}= $vals[0];
    }
}
# }}}

# {{{ CaseSensitive

=head2 CaseSensitive

Returns 1 if the current database's searches are case sensitive by default
Returns undef otherwise

=cut

sub CaseSensitive {
    my $self = shift;
    return(1);
}


# }}} 


# {{{ _MakeClauseCaseInsensitive

=head2 _MakeClauseCaseInsensitive FIELD OPERATOR VALUE

Takes a field, operator and value. performs the magic necessary to make
your database treat this clause as case insensitive.

Returns a FIELD OPERATOR VALUE triple.

=cut

sub _MakeClauseCaseInsensitive {
    my $self = shift;
    my $field = shift;
    my $operator = shift;
    my $value = shift;

    $field = "lower($field)";
    $value = lc($value);

    return ($field, $operator, $value,undef);
}


# }}}

# {{{ BeginTransaction

=head2 BeginTransaction

Tells DBIx::SearchBuilder to begin a new SQL transaction. This will
temporarily suspend Autocommit mode.

Emulates nested transactions, by keeping a transaction stack depth.

=cut

sub BeginTransaction {
    my $self = shift;
    $TRANSDEPTH++;
    if ($TRANSDEPTH > 1 ) {
        return ($TRANSDEPTH);
    } else {
       return($self->dbh->begin_work);
    }
}

# }}}

# {{{ Commit

=head2 Commit

Tells DBIx::SearchBuilder to commit the current SQL transaction. 
This will turn Autocommit mode back on.

=cut

sub Commit {
    my $self = shift;
    unless ($TRANSDEPTH) {Carp::confess("Attempted to commit a transaction with none in progress")};
    $TRANSDEPTH--;

    if ($TRANSDEPTH == 0 ) {
        return($self->dbh->commit);
    } else { #we're inside a transaction
        return($TRANSDEPTH);
    }
}

# }}}

# {{{ Rollback

=head2 Rollback [FORCE]

Tells DBIx::SearchBuilder to abort the current SQL transaction. 
This will turn Autocommit mode back on.

If this method is passed a true argument, stack depth is blown away and the outermost transaction is rolled back

=cut

sub Rollback {
    my $self = shift;
    my $force = shift || undef;
    #unless ($TRANSDEPTH) {Carp::confess("Attempted to rollback a transaction with none in progress")};
    $TRANSDEPTH--;

    if ($force) {
        $TRANSDEPTH = 0;
       return($self->dbh->rollback);
    }

    if ($TRANSDEPTH == 0 ) {
       return($self->dbh->rollback);
    } else { #we're inside a transaction
        return($TRANSDEPTH);
    }
}

# }}}

=head2 ForceRollback

Force the handle to rollback. Whether or not we're deep in nested transactions

=cut

sub ForceRollback {
    my $self = shift;
    $self->Rollback(1);
}


=head2 TransactionDepth

Return the current depth of the faked nested transaction stack.

=cut

sub TransactionDepth {
    my $self = shift;
    return ($TRANSDEPTH); 
}


# {{{ ApplyLimits

=head2 ApplyLimits STATEMENTREF ROWS_PER_PAGE FIRST_ROW

takes an SQL SELECT statement and massages it to return ROWS_PER_PAGE starting with FIRST_ROW;


=cut

sub ApplyLimits {
    my $self = shift;
    my $statementref = shift;
    my $per_page = shift;
    my $first = shift;

    my $limit_clause = '';

    if ( $per_page) {
        $limit_clause = " LIMIT ";
        if ( $first ) {
            $limit_clause .= $first . ", ";
        }
        $limit_clause .= $per_page;
    }

   $$statementref .= $limit_clause; 

}


# }}}


# {{{ Join

=head2 Join { Paramhash }

Takes a paramhash of everything Searchbuildler::Record does 
plus a parameter called 'SearchBuilder' that contains a ref 
to a SearchBuilder object'.

This performs the join.


=cut


sub Join {

    my $self = shift;
    my %args = (
        SearchBuilder => undef,
        TYPE          => 'normal',
        FIELD1        => undef,
        ALIAS1        => undef,
        TABLE2        => undef,
        FIELD2        => undef,
        ALIAS2        => undef,
        @_
    );

    my $sb = $args{'SearchBuilder'};


    if ( $args{'TYPE'} =~ /LEFT/i ) {
        my $alias = $sb->_GetAlias( $args{'TABLE2'} );

        $sb->{'left_joins'}{"$alias"}{'alias_string'} =
          " LEFT JOIN $args{'TABLE2'} $alias ";

        $sb->{'left_joins'}{"$alias"}{'criteria'}{'base_criterion'} =
          " $args{'ALIAS1'}.$args{'FIELD1'} = $alias.$args{'FIELD2'}";

        return ($alias);
    }
    else {
    $sb->DBIx::SearchBuilder::Limit(
          ENTRYAGGREGATOR => 'AND',
          QUOTEVALUE      => 0,
          ALIAS           => $args{'ALIAS1'},
          FIELD           => $args{'FIELD1'},
          VALUE           => $args{'ALIAS2'} . "." . $args{'FIELD2'},
          @_
    ); 
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
    #my $table = shift;

    # Prepend select query for DBs which allow DISTINCT on all column types.
    $$statementref = "SELECT DISTINCT main.* FROM $$statementref";

}

# }}}


# {{{ DistinctCount

=head2 DistinctCount STATEMENTREF 

takes an incomplete SQL SELECT statement and massages it to return a DISTINCT result set.


=cut

sub DistinctCount {
    my $self = shift;
    my $statementref = shift;

    # Prepend select query for DBs which allow DISTINCT on all column types.
    $$statementref = "SELECT COUNT(DISTINCT main.id) FROM $$statementref";

}

# }}}

=head2 Log MESSAGE

Takes a single argument, a message to log.

Currently prints that message to STDERR

=cut

sub Log {
	my $self = shift;
	my $msg = shift;
	warn $msg."\n";

}



=head2 DESTROY

When we get rid of the Searchbuilder::Handle, we need to disconnect from the database

=cut

  
sub DESTROY {
  my $self = shift;
  $self->Disconnect if $self->{'DisconnectHandleOnDestroy'};
  delete $DBIHandle{$self};
}


1;
__END__

# {{{ POD

=head1 SEE ALSO

perl(1), L<DBIx::SearchBuilder>

=cut

# }}}
