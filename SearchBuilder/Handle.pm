# $Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Handle.pm,v 1.7 2001/01/03 19:41:20 jesse Exp $
package DBIx::SearchBuilder::Handle;
use Carp;
use DBI;
use strict;
use vars qw($VERSION @ISA $DBIHandle);


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
  my(@cols, @vals);

#  my %seen; #only the *first* value is used - allows drivers to specify default
  while ( my $key = shift @pairs ) {
    my $value = shift @pairs;
#    next if $seen{$key}++;
    push @cols, $key;
    if ( defined($value) ) {
      $value = $self->safe_quote($value)
        unless ( $key eq 'Created' || $key eq 'LastUpdated' )
               && lc($value) eq 'now()';
      push @vals, $value;
    } else {
      push @vals, 'NULL';
    }
  }

  my $QueryString =
    "INSERT INTO $table (". join(", ", @cols). ") VALUES ".
    "(". join(", ", @vals). ")";
  #warn $QueryString;

  $self->SimpleQuery($QueryString);
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
	       User => undef,
	       Password => undef,
	       @_);
  
  my $dsn;
  
  $dsn = "dbi:$args{'Driver'}:dbname=$args{'Database'};host=$args{'Host'}";
  
  $DBIHandle = DBI->connect_cached($dsn, $args{'User'}, $args{'Password'}) || croak "Connect Failed $DBI::errstr\n" ;


  $self->dbh->{RaiseError}=1;
  $self->dbh->{PrintError}=1;
  return (1); 
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

=head2 dbh

Return the current DBI handle

=cut

# allow use of Handle as a synonym for DBH
*Handle=\&dbh;

sub dbh {
  my $self=shift;
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
  my $QueryString;
  
  # quote the value
  # TODO: We need some general way to escape SQL functions.
  $NewValue=$self->safe_quote($NewValue) unless ($is_sql);
  # build the query string
  $QueryString = "UPDATE $Table SET $Col = $NewValue WHERE id = $Record";
  
  
  my $sth = $self->dbh->prepare($QueryString);
  if (!$sth) {
    
    if ($main::debug) {
      die "Error:" . $self->dbh->errstr . "\n";
    }
    else {
      return (0);
  }
  }
  if (!$sth->execute) {
    if ($self->{'debug'}) {
      die "Error:" . $sth->errstr . "\n";
    }
    else {
      return(0);
    }
    
  }
  
  return (1); #Update Succeded
}

# }}}

# {{{ sub SimpleQuery

=head2 SimpleQuery QUERY_STRING

Execute the SQL string specified in QUERY_STRING

=cut

sub SimpleQuery  {
  my $self = shift;
  my $QueryString = shift;
  
  my $sth = $self->dbh->prepare($QueryString);
  if (!$sth) {
    if ($main::debug) {
      die "Error:" . $self->dbh->errstr . "\n";
    }
    else {
      return (0);
    }
  }
  if (!$sth->execute) {
    if ($self->{'debug'}) {
      die "Error:" . $sth->errstr . "\n";
    }
    else {
      return(0);
    }
    
  }
  return ($sth);
  
}

# }}}

# {{{ sub FetchResult

=head2 FetchResult

Takes a SELECT query as a string.
Returns the first row as an array

=cut 

sub FetchResult {
  my $self = shift;
  my $query = shift;
  my $sth = $self->SimpleQuery($query);

  return ($sth->fetchrow);
}
# }}}

# {{{ sub safe_quote 

=head2 safe_quote IN_VAL

If IN_VAL is null, turn it into an empty quoted string. otherwise, use the DBI quote function. Returns the new string.

=cut

sub safe_quote  {
   my $self = shift;
   my $in_val = shift;
   my ($out_val);
   if (!defined $in_val) {
     return ("''");
     
   }
   else {
     $out_val = $self->dbh->quote($in_val);
     
   }
   return("$out_val");
   
}

# }}}
 
 
 
# Autoload methods go after =cut, and are processed by the autosplit program.
 
 1;
__END__

# {{{ POD



=head1 SEE ALSO

perl(1), L<DBIx::SearchBuilder>

=cut

# }}} POD

