# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle.pm,v 1.21 2002/01/28 06:11:37 jesse Exp $
package DBIx::SearchBuilder::Handle;
use Carp;
use DBI;
use strict;
use vars qw($VERSION @ISA $DBIHandle $DEBUG);


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
    warn $QueryString if $DEBUG;
    
    my $sth =  $self->SimpleQuery($QueryString, @bind);
    return ($sth);
  }
# }}}

# {{{ sub Connect 

=head2 Connect PARAMHASH: Driver, Database, Host, User, Password

Takes a paramhash and connects to your DBI datasource. 


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
	       @_);

  $self->BuildDSN(%args);

  my $handle = DBI->connect($self->DSN, $args{'User'}, $args{'Password'}) || croak "Connect Failed $DBI::errstr\n" ;

  #Set the handle 
  $self->dbh($handle);

  return (1); 
}
# }}}

# {{{ BuildDSN

=item  BuildDSN PARAMHASH

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
  $dsn .= ";sid=$args{'SID'}" if ( defined $args{'SID'});
  $dsn .= ";host=$args{'Host'}" if (defined$args{'Host'});
  $dsn .= ";port=$args{'Port'}" if (defined $args{'Port'});
  $dsn .= ";requiressl=1" if (defined $args{'RequireSSL'});

  $self->{'dsn'}= $dsn;
}

# }}}

# {{{ DSN

=item DSN

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
  return ($self->dbh->disconnect());
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
  $DBIHandle = shift if (@_);

  return($DBIHandle);
}

# }}}

# {{{ sub UpdateTableValue 

=head2 UpdateRecordValue 

Takes a hash with fields: Table, Column, Value PrimaryKeys, and 
IsSqlFunction.  Table, and Column should be obvious, Value is where you 
set the new value you want the column to have. The primary_keys field should 
be the lvalue of DBIx::SearchBuilder::Record::PrimaryKeys().  Finally 
sql_function_p is set when the Value is a SQL function.  For example, you 
might have ('Value'=>'PASSWORD(string)'), by setting sql_function_p that 
string will be inserted into the query directly rather then as a binding. 

=cut

sub UpdateRecordValue {
  my $self = shift;
  my %args = @_;

  my @bind   = ();
  my $query  = 'UPDATE ' . $args{'Table'}  . ' ';
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
	    return (undef);
	}
    }
    unless ($sth->execute(@bind_values)) {
	if ($DEBUG) {
	    die "$self couldn't execute the query '$QueryString'" . 
	      $self->dbh->errstr . "\n";
	    
	}
	else {
	    warn "$self couldn't execute the query '$QueryString'" . 
	      $self->dbh->errstr . "\n";
	    return(undef);
	}
	
    }
    return ($sth);
    
    
  }

# }}}

# {{{ sub FetchResult

=head2 FetchResult QUERY, [ BIND_VALUE, ... ]

Takes a SELECT query as a string, along with an array of BIND_VALUEs
Returns the first row as an array

=cut 

sub FetchResult {
  my $self = shift;
  my $query = shift;
  my @bind_values = @_;
  my $sth = $self->SimpleQuery($query, @bind_values);
  
  return ($sth->fetchrow);
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

# {{{ BeginTransaction

=head2 BeginTransaction

Tells DBIx::SearchBuilder to begin a new SQL transaction. This will
temporarily suspend Autocommit mode.

=cut

sub BeginTransaction {
    my $self = shift;
    return($self->SimpleQuery('BEGIN'));
}

# }}}

# {{{ Commit

=head2 Commit

Tells DBIx::SearchBuilder to commit the current SQL transaction. 
This will turn Autocommit mode back on.

=cut

sub Commit {
    my $self = shift;
    return($self->SimpleQuery('COMMIT'));
}

# }}}

# {{{ Rollback

=head2 Rollback

Tells DBIx::SearchBuilder to abort the current SQL transaction. 
This will turn Autocommit mode back on.

=cut

sub Rollback {
    my $self = shift;
    return($self->SimpleQuery('ROLLBACK'));
}

# }}}

1;
__END__

# {{{ POD

=head1 SEE ALSO

perl(1), L<DBIx::SearchBuilder>

=cut

# }}}
