# $Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Handle.pm,v 1.12 2001/01/25 03:06:31 jesse Exp $
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
	       Host => 'localhost',
	       Port => undef,
	       User => undef,
	       Password => undef,
	       @_);
  
  my $dsn;
  
  $dsn = "dbi:$args{'Driver'}:dbname=$args{'Database'};host=$args{'Host'}";
  $dsn .= ";port=$args{'Port'}" if defined($args{'Port'});

  my $handle = DBI->connect_cached($dsn, $args{'User'}, $args{'Password'}) || croak "Connect Failed $DBI::errstr\n" ;

  #Set the handle 
  $self->dbh($handle);

  return (1); 
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

=head2 UpdateTableValue TABLE COLUMN NEW_VALUE RECORD_ID IS_SQL

Update column COLUMN of table TABLE where the record id = RECORD_ID.  if IS_SQL is set,
don\'t quote the NEW_VALUE

=cut

sub UpdateTableValue  {
    my $self = shift;
    
    my $Table = shift;
    my $Col = shift;
    my $NewValue = shift;
    my $Record = shift;
    my $is_sql = shift;

    if ( $is_sql ) {
	return ($self->SimpleQuery( "UPDATE $Table SET $Col = $NewValue WHERE id = ?",
				    $Record
				  ));
    } else { 
	return ($self->SimpleQuery( "UPDATE $Table SET $Col = ? WHERE id = ?",
				    $NewValue, $Record
				  ));
    }
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

 
# Autoload methods go after =cut, and are processed by the autosplit program.
 
 1;
__END__

# {{{ POD

=head1 SEE ALSO

perl(1), L<DBIx::SearchBuilder>

=cut

# }}}
