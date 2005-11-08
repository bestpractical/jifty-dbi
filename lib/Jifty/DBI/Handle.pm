package Jifty::DBI::Handle;
use strict;
use Carp               ();
use DBI                ();
use Class::ReturnValue ();
use Encode             ();

use base qw/Jifty::DBI::HasFilters/;

use vars qw(%DBIHandle $PrevHandle $DEBUG $TRANSDEPTH);

$TRANSDEPTH = 0;

our $VERSION = '0.01';

=head1 NAME

Jifty::DBI::Handle - Perl extension which is a generic DBI handle

=head1 SYNOPSIS

  use Jifty::DBI::Handle;

  my $handle = Jifty::DBI::Handle->new();
  $handle->connect( Driver => 'mysql',
                    Database => 'dbname',
                    Host => 'hostname',
                    User => 'dbuser',
                    Password => 'dbpassword');
  # now $handle isa Jifty::DBI::Handle::mysql                    
 
=head1 DESCRIPTION

This class provides a wrapper for DBI handles that can also perform a number of additional functions.
 
=cut

=head2 new

Generic constructor

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless( $self, $class );

    @{ $self->{'StatementLog'} } = ();
    return $self;
}

=head2 connect PARAMHASH: Driver, Database, Host, User, Password

Takes a paramhash and connects to your DBI datasource. 

If you created the handle with 
     Jifty::DBI::Handle->new
and there is a Jifty::DBI::Handle::(Driver) subclass for the driver you have chosen,
the handle will be automatically "upgraded" into that subclass.

=cut

sub connect {
    my $self = shift;

    my %args = (
        driver     => undef,
        database   => undef,
        host       => undef,
        sid        => undef,
        port       => undef,
        user       => undef,
        password   => undef,
        requiressl => undef,
        @_
    );

    if ( $args{'driver'}
        && !$self->isa( 'Jifty::DBI::Handle::' . $args{'driver'} ) )
    {
        if ( $self->_upgrade_handle( $args{'driver'} ) ) {
            return ( $self->connect(%args) );
        }
    }

    my $dsn = $self->DSN || '';

# Setting this actually breaks old RT versions in subtle ways. So we need to explicitly call it

    $self->build_dsn(%args);

    # Only connect if we're not connected to this source already
    if ( ( !$self->dbh ) || ( !$self->dbh->ping ) || ( $self->DSN ne $dsn ) )
    {
        my $handle
            = DBI->connect( $self->DSN, $args{'user'}, $args{'password'} )
            || Carp::croak "Connect Failed $DBI::errstr\n";

#databases do case conversion on the name of columns returned.
#actually, some databases just ignore case. this smashes it to something consistent
        $handle->{FetchHashKeyName} = 'NAME_lc';

        #Set the handle
        $self->dbh($handle);

        return (1);
    }

    return (undef);

}

=head2 _upgrade_handle DRIVER

This private internal method turns a plain Jifty::DBI::Handle into one
of the standard driver-specific subclasses.

=cut

sub _upgrade_handle {
    my $self = shift;

    my $driver = shift;
    my $class  = 'Jifty::DBI::Handle::' . $driver;
    eval "require $class";
    return if $@;

    bless $self, $class;
    return 1;
}

=head2 build_dsn PARAMHASH

Takes a bunch of parameters:  

Required: Driver, Database,
Optional: Host, Port and RequireSSL

Builds a DSN suitable for a DBI connection

=cut

sub build_dsn {
    my $self = shift;
    my %args = (
        driver     => undef,
        database   => undef,
        host       => undef,
        port       => undef,
        sid        => undef,
        requiressl => undef,
        @_
    );

    my $dsn = "dbi:$args{'driver'}:dbname=$args{'database'}";
    $dsn .= ";sid=$args{'sid'}" if ( defined $args{'sid'} && $args{'sid'} );
    $dsn .= ";host=$args{'host'}"
        if ( defined $args{'host'} && $args{'host'} );
    $dsn .= ";port=$args{'port'}"
        if ( defined $args{'port'} && $args{'port'} );
    $dsn .= ";requiressl=1"
        if ( defined $args{'requiressl'} && $args{'requiressl'} );

    $self->{'dsn'} = $dsn;
}

=head2 DSN

Returns the DSN for this database connection.

=cut

sub DSN {
    my $self = shift;
    return ( $self->{'dsn'} );
}

=head2 raise_error [MODE]

Turns on the Database Handle's RaiseError attribute.

=cut

sub raise_error {
    my $self = shift;
    $self->dbh->{RaiseError} = shift if (@_);
    return $self->dbh->{RaiseError};
}

=head2 print_error [MODE]

Turns on the Database Handle's PrintError attribute.

=cut

sub print_error {
    my $self = shift;
    $self->dbh->{PrintError} = shift if (@_);
    return $self->dbh->{PrintError};
}

=head2 log_sql_statements BOOL

Takes a boolean argument. If the boolean is true, it will log all SQL
statements, as well as their invocation times and execution times.

Returns whether we're currently logging or not as a boolean

=cut

sub log_sql_statements {
    my $self = shift;
    if (@_) {
        require Time::HiRes;
        $self->{'_dologsql'} = shift;
    }
    return ( $self->{'_dologsql'} );
}

=head2 _log_sql_statement STATEMENT DURATION

add an SQL statement to our query log

=cut

sub _log_sql_statement {
    my $self      = shift;
    my $statement = shift;
    my $duration  = shift;
    my @bind      = @_;
    push @{ $self->{'StatementLog'} },
        ( [ Time::HiRes::time(), $statement, [@bind], $duration ] );

}

=head2 clear_sql_statement_log

Clears out the SQL statement log. 

=cut

sub clear_sql_statement_log {
    my $self = shift;
    @{ $self->{'StatementLog'} } = ();
}

=head2 sql_statement_log

Returns the current SQL statement log as an array of arrays. Each entry is a triple of 

(Time,  Statement, Duration)

=cut

sub sql_statement_log {
    my $self = shift;
    return ( @{ $self->{'StatementLog'} } );

}

=head2 auto_commit [MODE]

Turns on the Database Handle's Autocommit attribute.

=cut

sub auto_commit {
    my $self = shift;

    my $mode = 1;
    $mode = shift if (@_);

    $self->dbh->{AutoCommit} = $mode;
}

=head2 disconnect

disconnect from your DBI datasource

=cut

sub disconnect {
    my $self = shift;
    if ( $self->dbh ) {
        return ( $self->dbh->disconnect() );
    }
    else {
        return;
    }
}

=head2 dbh [HANDLE]

Return the current DBI handle. If we're handed a parameter, make the database handle that.

=cut

sub dbh {
    my $self = shift;

    #If we are setting the database handle, set it.
    $DBIHandle{$self} = $PrevHandle = shift if (@_);

    return ( $DBIHandle{$self} ||= $PrevHandle );
}

=head2 insert $table_NAME @KEY_VALUE_PAIRS

Takes a table name and a set of key-value pairs in an array. splits the key value pairs, constructs an INSERT statement and performs the insert. Returns the row_id of this row.

=cut

sub insert {
    my ( $self, $table, @pairs ) = @_;
    my ( @cols, @vals,  @bind );

#my %seen; #only the *first* value is used - allows drivers to specify default
    while ( my $key = shift @pairs ) {
        my $value = shift @pairs;

        # next if $seen{$key}++;
        push @cols, $key;
        push @vals, '?';
        push @bind, $value;
    }

    my $query_string = "INSERT INTO $table ("
        . CORE::join( ", ", @cols )
        . ") VALUES " . "("
        . CORE::join( ", ", @vals ) . ")";

    my $sth = $self->simple_query( $query_string, @bind );
    return ($sth);
}

=head2 update_record_value 

Takes a hash with columns: Table, Column, Value PrimaryKeys, and 
IsSQLFunction.  Table, and Column should be obvious, Value is where you 
set the new value you want the column to have. The primary_keys column should 
be the lvalue of Jifty::DBI::Record::PrimaryKeys().  Finally 
IsSQLFunction is set when the Value is a SQL function.  For example, you 
might have ('Value'=>'PASSWORD(string)'), by setting IsSQLFunction that 
string will be inserted into the query directly rather then as a binding. 

=cut

sub update_record_value {
    my $self = shift;
    my %args = (
        table           => undef,
        column          => undef,
        is_sql_function => undef,
        primary_keys    => undef,
        @_
    );

    my @bind  = ();
    my $query = 'UPDATE ' . $args{'table'} . ' ';
    $query .= 'SET ' . $args{'column'} . '=';

    ## Look and see if the column is being updated via a SQL function.
    if ( $args{'is_sql_function'} ) {
        $query .= $args{'value'} . ' ';
    }
    else {
        $query .= '? ';
        push( @bind, $args{'value'} );
    }

    ## Constructs the where clause.
    my $where = 'WHERE ';
    foreach my $key ( keys %{ $args{'primary_keys'} } ) {
        $where .= $key . "=?" . " AND ";
        push( @bind, $args{'primary_keys'}{$key} );
    }
    $where =~ s/AND\s$//;

    my $query_str = $query . $where;
    return ( $self->simple_query( $query_str, @bind ) );
}

=head2 update_table_value table COLUMN NEW_value RECORD_ID IS_SQL

Update column COLUMN of table table where the record id = RECORD_ID.  if IS_SQL is set,
don\'t quote the NEW_VALUE

=cut

sub update_table_value {
    my $self = shift;

    ## This is just a wrapper to update_record_value().
    my %args = ();
    $args{'table'}           = shift;
    $args{'column'}          = shift;
    $args{'value'}           = shift;
    $args{'primary_keys'}    = shift;
    $args{'is_sql_function'} = shift;

    return $self->update_record_value(%args);
}

=head2 simple_query QUERY_STRING, [ BIND_VALUE, ... ]

Execute the SQL string specified in QUERY_STRING

=cut

sub simple_query {
    my $self        = shift;
    my $query_string = shift;
    my @bind_values;
    @bind_values = (@_) if (@_);

    my $sth = $self->dbh->prepare($query_string);
    unless ($sth) {
        if ($DEBUG) {
            die "$self couldn't prepare the query '$query_string'"
                . $self->dbh->errstr . "\n";
        }
        else {
            warn "$self couldn't prepare the query '$query_string'"
                . $self->dbh->errstr . "\n";
            my $ret = Class::ReturnValue->new();
            $ret->as_error(
                errno   => '-1',
                message => "Couldn't prepare the query '$query_string'."
                    . $self->dbh->errstr,
                do_backtrace => undef
            );
            return ( $ret->return_value );
        }
    }

    # Check @bind_values for HASH refs
    for ( my $bind_idx = 0; $bind_idx < scalar @bind_values; $bind_idx++ ) {
        if ( ref( $bind_values[$bind_idx] ) eq "HASH" ) {
            my $bhash = $bind_values[$bind_idx];
            $bind_values[$bind_idx] = $bhash->{'value'};
            delete $bhash->{'value'};
            $sth->bind_param( $bind_idx + 1, undef, $bhash );
        }

        # Some databases, such as Oracle fail to cope if it's a perl utf8
        # string. they desperately want bytes.
        Encode::_utf8_off( $bind_values[$bind_idx] );
    }

    my $basetime;
    if ( $self->log_sql_statements ) {
        $basetime = Time::HiRes::time();
    }
    my $executed;
    {
        no warnings 'uninitialized';    # undef in bind_values makes DBI sad
        eval { $executed = $sth->execute(@bind_values) };
    }
    if ( $self->log_sql_statements ) {
        $self->_log_sql_statement( $query_string,
            Time::HiRes::time() - $basetime, @bind_values );

    }

    if ( $@ or !$executed ) {
        if ($DEBUG) {
            die "$self couldn't execute the query '$query_string'"
                . $self->dbh->errstr . "\n";

        }
        else {
            Carp::cluck "$self couldn't execute the query '$query_string'";

            my $ret = Class::ReturnValue->new();
            $ret->as_error(
                errno   => '-1',
                message => "Couldn't execute the query '$query_string'"
                    . $self->dbh->errstr,
                do_backtrace => undef
            );
            return ( $ret->return_value );
        }

    }
    return ($sth);

}

=head2 fetch_result QUERY, [ BIND_VALUE, ... ]

Takes a SELECT query as a string, along with an array of BIND_VALUEs
If the select succeeds, returns the first row as an array.
Otherwise, returns a Class::ResturnValue object with the failure loaded
up.

=cut 

sub fetch_result {
    my $self        = shift;
    my $query       = shift;
    my @bind_values = @_;
    my $sth         = $self->simple_query( $query, @bind_values );
    if ($sth) {
        return ( $sth->fetchrow );
    }
    else {
        return ($sth);
    }
}

=head2 binary_safe_blobs

Returns 1 if the current database supports BLOBs with embedded nulls.
Returns undef if the current database doesn't support BLOBs with embedded nulls

=cut

sub binary_safe_blobs {
    my $self = shift;
    return (1);
}

=head2 knows_blobs

Returns 1 if the current database supports inserts of BLOBs automatically.
Returns undef if the current database must be informed of BLOBs for inserts.

=cut

sub knows_blobs {
    my $self = shift;
    return (1);
}

=head2 blob_params column_NAME column_type

Returns a hash ref for the bind_param call to identify BLOB types used by 
the current database for a particular column type.                 

=cut

sub blob_params {
    my $self = shift;

    # Don't assign to key 'value' as it is defined later.
    return ( {} );
}

=head2 database_version

Returns the database's version.

If argument C<short> is true returns short variant, in other
case returns whatever database handle/driver returns. By default
returns short version, e.g. '4.1.23' or '8.0-rc4'.

Returns empty string on error or if database couldn't return version.

The base implementation uses a C<SELECT VERSION()>

=cut

sub database_version {
    my $self = shift;
    my %args = ( short => 1, @_ );

    unless ( defined $self->{'database_version'} ) {

        # turn off error handling, store old values to restore later
        my $re = $self->raise_error;
        $self->raise_error(0);
        my $pe = $self->print_error;
        $self->print_error(0);

        my $statement = "SELECT VERSION()";
        my $sth       = $self->simple_query($statement);

        my $ver = '';
        $ver = ( $sth->fetchrow_arrayref->[0] || '' ) if $sth;
        $ver =~ /(\d+(?:\.\d+)*(?:-[a-z0-9]+)?)/i;
        $self->{'database_version'}       = $ver;
        $self->{'database_version_short'} = $1 || $ver;

        $self->raise_error($re);
        $self->print_error($pe);
    }

    return $self->{'database_version_short'} if $args{'short'};
    return $self->{'database_version'};
}

=head2 case_sensitive

Returns 1 if the current database's searches are case sensitive by default
Returns undef otherwise

=cut

sub case_sensitive {
    my $self = shift;
    return (1);
}

=head2 _make_clause_case_insensitive column operator VALUE

Takes a column, operator and value. performs the magic necessary to make
your database treat this clause as case insensitive.

Returns a column operator value triple.

=cut

sub _make_clause_case_insensitive {
    my $self     = shift;
    my $column    = shift;
    my $operator = shift;
    my $value    = shift;

    if ( $value !~ /^\d+$/ ) {    # don't downcase integer values
        $column = "lower($column)";
        $value = lc($value);
    }
    return ( $column, $operator, $value, undef );
}

=head2 begin_transaction

Tells Jifty::DBI to begin a new SQL transaction. This will
temporarily suspend Autocommit mode.

Emulates nested transactions, by keeping a transaction stack depth.

=cut

sub begin_transaction {
    my $self = shift;
    $TRANSDEPTH++;
    if ( $TRANSDEPTH > 1 ) {
        return ($TRANSDEPTH);
    }
    else {
        return ( $self->dbh->begin_work );
    }
}

=head2 commit

Tells Jifty::DBI to commit the current SQL transaction. 
This will turn Autocommit mode back on.

=cut

sub commit {
    my $self = shift;
    unless ($TRANSDEPTH) {
        Carp::confess(
            "Attempted to commit a transaction with none in progress");
    }
    $TRANSDEPTH--;

    if ( $TRANSDEPTH == 0 ) {
        return ( $self->dbh->commit );
    }
    else {    #we're inside a transaction
        return ($TRANSDEPTH);
    }
}

=head2 rollback [FORCE]

Tells Jifty::DBI to abort the current SQL transaction. 
This will turn Autocommit mode back on.

If this method is passed a true argument, stack depth is blown away and the outermost transaction is rolled back

=cut

sub rollback {
    my $self  = shift;
    my $force = shift || undef;

#unless ($TRANSDEPTH) {Carp::confess("Attempted to rollback a transaction with none in progress")};
    $TRANSDEPTH--;

    if ($force) {
        $TRANSDEPTH = 0;
        return ( $self->dbh->rollback );
    }

    if ( $TRANSDEPTH == 0 ) {
        return ( $self->dbh->rollback );
    }
    else {    #we're inside a transaction
        return ($TRANSDEPTH);
    }
}

=head2 force_rollback

Force the handle to rollback. Whether or not we're deep in nested transactions

=cut

sub force_rollback {
    my $self = shift;
    $self->rollback(1);
}

=head2 transaction_depthh

Return the current depth of the faked nested transaction stack.

=cut

sub transaction_depthh {
    my $self = shift;
    return ($TRANSDEPTH);
}

=head2 apply_limits STATEMENTREF ROWS_PER_PAGE FIRST_ROW

takes an SQL SELECT statement and massages it to return ROWS_PER_PAGE starting with FIRST_ROW;


=cut

sub apply_limits {
    my $self         = shift;
    my $statementref = shift;
    my $per_page     = shift;
    my $first        = shift;

    my $limit_clause = '';

    if ($per_page) {
        $limit_clause = " LIMIT ";
        if ($first) {
            $limit_clause .= $first . ", ";
        }
        $limit_clause .= $per_page;
    }

    $$statementref .= $limit_clause;

}

=head2 join { Paramhash }

Takes a paramhash of everything Searchbuildler::Record does plus a
parameter called 'collection' that contains a ref to a
L<Jifty::DBI::Collection> object'.

This performs the join.


=cut

sub join {

    my $self = shift;
    my %args = (
        collection => undef,
        type       => 'normal',
        column1    => 'main',
        alias1     => undef,
        table2     => undef,
        column2    => undef,
        alias2     => undef,
        expression => undef,
        @_
    );

    my $string;

    my $alias;

    #If we're handed in an alias2, we need to go remove it from the
    # Aliases array.  Basically, if anyone generates an alias and then
    # tries to use it in a join later, we want to be smart about creating
    # joins, so we need to go rip it out of the old aliases table and drop
    # it in as an explicit join
    if ( $args{'alias2'} ) {

        # this code is slow and wasteful, but it's clear.
        my @aliases = @{ $args{'collection'}->{'aliases'} };
        my @new_aliases;
        foreach my $old_alias (@aliases) {
            if ( $old_alias =~ /^(.*?) ($args{'alias2'})$/ ) {
                $args{'table2'} = $1;
                $alias = $2;

            }
            else {
                push @new_aliases, $old_alias;
            }
        }

# If we found an alias, great. let's just pull out the table and alias for the other item
        unless ($alias) {

            # if we can't do that, can we reverse the join and have it work?
            my $a1 = $args{'alias1'};
            my $f1 = $args{'column1'};
            $args{'alias1'}  = $args{'alias2'};
            $args{'column1'} = $args{'column2'};
            $args{'alias2'}  = $a1;
            $args{'column2'} = $f1;

            @aliases     = @{ $args{'collection'}->{'aliases'} };
            @new_aliases = ();
            foreach my $old_alias (@aliases) {
                if ( $old_alias =~ /^(.*?) ($args{'alias2'})$/ ) {
                    $args{'table2'} = $1;
                    $alias = $2;

                }
                else {
                    push @new_aliases, $old_alias;
                }
            }

        }

        if ( !$alias || $args{'alias1'} ) {
            return ( $self->_normal_join(%args) );
        }

        $args{'collection'}->{'aliases'} = \@new_aliases;
    }

    else {
        $alias = $args{'collection'}->_get_alias( $args{'table2'} );

    }

    if ( $args{'type'} =~ /LEFT/i ) {

        $string = " LEFT JOIN " . $args{'table2'} . " $alias ";

    }
    else {

        $string = " JOIN " . $args{'table2'} . " $alias ";

    }

    my $criterion;
    if ( $args{'expression'} ) {
        $criterion = $args{'expression'};
    }
    else {
        $criterion = $args{'alias1'} . "." . $args{'column1'};
    }

    $args{'collection'}->{'leftjoins'}{"$alias"}{'alias_string'} = $string;
    $args{'collection'}->{'leftjoins'}{"$alias"}{'entry_aggregator'}
          = $args{'entry_aggregator'} if ( $args{'entry_aggregator'} );
    $args{'collection'}->{'leftjoins'}{"$alias"}{'depends_on'}
        = $args{'alias1'};
    $args{'collection'}->{'leftjoins'}{"$alias"}{'criteria'}
        { 'criterion' . $args{'collection'}->{'criteria_count'}++ }
        = " $alias.$args{'column2'} = $criterion";

    return ($alias);
}

sub _normal_join {

    my $self = shift;
    my %args = (
        collection => undef,
        type       => 'normal',
        column1    => undef,
        alias1     => undef,
        table2     => undef,
        column2    => undef,
        alias2     => undef,
        @_
    );

    my $sb = $args{'collection'};

    if ( $args{'type'} =~ /LEFT/i ) {
        my $alias = $sb->_get_alias( $args{'table2'} );

        $sb->{'leftjoins'}{"$alias"}{'alias_string'}
            = " LEFT JOIN $args{'table2'} $alias ";

        $sb->{'leftjoins'}{"$alias"}{'criteria'}{'base_criterion'}
            = " $args{'alias1'}.$args{'column1'} = $alias.$args{'column2'}";

        return ($alias);
    }
    else {
        $sb->Jifty::DBI::Collection::limit(
            entry_aggregator => 'AND',
            quote_value      => 0,
            alias            => $args{'alias1'},
            column           => $args{'column1'},
            value            => $args{'alias2'} . "." . $args{'column2'},
            @_
        );
    }
}

# this code is all hacky and evil. but people desperately want _something_ and I'm
# super tired. refactoring gratefully appreciated.

sub _build_joins {
    my $self = shift;
    my $sb   = shift;
    my %seen_aliases;

    $seen_aliases{'main'} = 1;

    # We don't want to get tripped up on a dependency on a simple alias.
    foreach my $alias ( @{ $sb->{'aliases'} } ) {
        if ( $alias =~ /^(.*?)\s+(.*?)$/ ) {
            $seen_aliases{$2} = 1;
        }
    }

    my $join_clause = $sb->table . " main ";

    my @keys = ( keys %{ $sb->{'leftjoins'} } );
    my %seen;

    while ( my $join = shift @keys ) {
        if ( !$sb->{'leftjoins'}{$join}{'depends_on'}
            || $seen_aliases{ $sb->{'leftjoins'}{$join}{'depends_on'} } )
        {
            $join_clause .= $sb->{'leftjoins'}{$join}{'alias_string'} . " ON ";
          
            my @criteria = values %{ $sb->{'leftjoins'}{$join}{'criteria'} } ;
            my $entry_aggregator =  $sb->{'leftjoins'}{$join}{'entry_aggregator'}  || 'AND';
            my $criteria = CORE::join( " $entry_aggregator ", map { " ( $_ ) " } @criteria);  

            $join_clause .= "( ".$criteria. " ) ";
            $join_clause = "(" .$join_clause .")";


            $seen_aliases{$join} = 1;
        }
        else {
            push( @keys, $join );
            die "Unsatisfied dependency chain in joins @keys"
                if $seen{"@keys"}++;
        }

    }
    return ( CORE::join( ", ", ( $join_clause, @{ $sb->{'aliases'} } ) ) );

}

=head2 distinct_query STATEMENTREF 

takes an incomplete SQL SELECT statement and massages it to return a DISTINCT result set.


=cut

sub distinct_query {
    my $self         = shift;
    my $statementref = shift;

    #my $table = shift;

    # Prepend select query for DBs which allow DISTINCT on all column types.
    $$statementref = "SELECT DISTINCT main.* FROM $$statementref";

}

=head2 distinct_count STATEMENTREF 

takes an incomplete SQL SELECT statement and massages it to return a DISTINCT result set.


=cut

sub distinct_count {
    my $self         = shift;
    my $statementref = shift;

    # Prepend select query for DBs which allow DISTINCT on all column types.
    $$statementref = "SELECT COUNT(DISTINCT main.id) FROM $$statementref";

}

=head2 Log MESSAGE

Takes a single argument, a message to log.

Currently prints that message to STDERR

=cut

sub log {
    my $self = shift;
    my $msg  = shift;
    warn $msg . "\n";

}

=head2 DESTROY

When we get rid of the L<Jifty::DBI::Handle>, we need to disconnect
from the database

=cut

sub DESTROY {
    my $self = shift;
    $self->disconnect;
    delete $DBIHandle{$self};
}

1;
__END__


=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), L<Jifty::DBI>

=cut

