package Jifty::DBI::Record;

use strict;
use warnings;

use vars qw($AUTOLOAD);
use Class::ReturnValue  ();
use Lingua::EN::Inflect ();
use Jifty::DBI::Column  ();
use UNIVERSAL::require  ();

use base qw/
    Class::Data::Inheritable
    Jifty::DBI::HasFilters
    /;

our $VERSION = '0.01';


Jifty::DBI::Record->mk_classdata('COLUMNS');

=head1 NAME

Jifty::DBI::Record - Superclass for records loaded by Jifty::DBI::Collection

=head1 SYNOPSIS

  package MyRecord;
  use base qw/Jifty::DBI::Record/;
  
=head1 DESCRIPTION

Jifty::DBI::Record encapuslates records and tables as part of the L<Jifty::DBI> 
object-relational mapper.

=head1 METHODS

=head2 new 

Instantiate a new, empty record object.

=cut

sub new {
    my $proto = shift;

    my $class = ref($proto) || $proto;
    my $self = {};
    bless( $self, $class );

    $self->_init_columns() unless $self->COLUMNS;
    $self->input_filters('Jifty::DBI::Filter::Truncate');

    $self->_init(@_);

    return $self;
}

# Not yet documented here.  Should almost certainly be overloaded.
sub _init {
    my $self   = shift;
    my $handle = shift;
    $self->_handle($handle);
}

=head2 id

Returns this row's primary key.

=cut

sub id {
    my $pkey = $_[0]->_primary_key();
    my $ret  = $_[0]->{'values'}->{$pkey};
    return $ret;
}

=head2 primary_keys

Return a hash of the values of our primary keys for this function.

=cut

sub primary_keys {
    my $self = shift;
    my %hash
        = map { $_ => $self->{'values'}->{$_} } @{ $self->_primary_keys };
    return (%hash);
}

sub DESTROY {
    return 1;
}

sub AUTOLOAD {
    my $self = $_[0];

    $self->_init_columns() unless $self->COLUMNS;

    my ( $column_name, $action ) = $self->_parse_autoload_method($AUTOLOAD);

    unless ( $action and $column_name ) {
        my ( $package, $filename, $line ) = caller;
        die "$AUTOLOAD Unimplemented in $package. ($filename line $line) \n";
    }

    my $column = $self->column($column_name);

    unless ($column) {
        my ( $package, $filename, $line ) = caller;
        die "$AUTOLOAD Unimplemented in $package. ($filename line $line) \n";
    }

    no strict 'refs';    # We're going to be defining subs
    if ( $action eq 'read' ) {
        return '' unless $column->readable;

        if ( UNIVERSAL::isa( $column->refers_to, "Jifty::DBI::Record" ) ) {
            *{$AUTOLOAD} = sub {
                $_[0]->_to_record( $column_name,
                    $_[0]->__value($column_name) );
            };
        }
        elsif (
            UNIVERSAL::isa( $column->refers_to, "Jifty::DBI::Collection" ) )
        {
            *{$AUTOLOAD} = sub { $_[0]->_collection_value($column_name) };
        }
        else {
            *{$AUTOLOAD} = sub { return ( $_[0]->_value($column_name) ) };
        }
        goto &$AUTOLOAD;
    }
    elsif ( $action eq 'write' ) {
        return ( 0, 'Immutable column' ) unless $column->writable;

        if ( UNIVERSAL::isa( $column->refers_to, "Jifty::DBI::Record" ) ) {
            *{$AUTOLOAD} = sub {
                my $self = shift;
                my $val  = shift;

                $val = $val->id
                    if UNIVERSAL::isa( $val, 'Jifty::DBI::Record' );
                return (
                    $self->_set( column => $column_name, value => $val ) );
            };
        }
        else {
            *{$AUTOLOAD} = sub {
                return (
                    $_[0]->_set( column => $column_name, value => $_[1] ) );
            };
        }
        goto &$AUTOLOAD;
    }
    elsif ( $action eq 'validate' ) {
        *{$AUTOLOAD}
            = sub { return ( $_[0]->_validate( $column_name, $_[1] ) ) };
        goto &$AUTOLOAD;
    }

    else {
        my ( $package, $filename, $line ) = caller;
        die "$AUTOLOAD Unimplemented in $package. ($filename line $line) \n";
    }

}

=head2 _parse_autoload_method $AUTOLOAD

Parses autoload methods and attempts to determine if they're 
set, get or validate calls.

Returns a tuple of (COLUMN_NAME, ACTION);

=cut

sub _parse_autoload_method {
    my $self   = shift;
    my $method = shift;

    my ( $column_name, $action );

    if ( $method =~ /^.*::set_(\w+)$/o ) {
        $column_name = $1;
        $action      = 'write';
    }
    elsif ( $method =~ /^.*::validate_(\w+)$/o ) {
        $column_name = $1;
        $action      = 'validate';

    }
    elsif ( $method =~ /^.*::(\w+)$/o ) {
        $column_name = $1;
        $action      = 'read';

    }
    return ( $column_name, $action );

}

=head2 _accessible COLUMN ATTRIBUTE

Private method. 

DEPRECATED

Returns undef unless C<COLUMN> has a true value for C<ATTRIBUTE>.

Otherwise returns C<COLUMN>'s value for that attribute.


=cut

sub _accessible {
    my $self        = shift;
    my $column_name = shift;
    my $attribute   = lc( shift || '' );

    my $col = $self->column($column_name);
    return undef unless ( $col and $col->can($attribute) );
    return $col->$attribute();

}

=head2 _primary_keys

Return our primary keys. (Subclasses should override this, but our
default is that we have one primary key, named 'id'.)

=cut

sub _primary_keys {
    my $self = shift;
    return ['id'];
}

sub _primary_key {
    my $self  = shift;
    my $pkeys = $self->_primary_keys();
    die "No primary key" unless ( ref($pkeys) eq 'ARRAY' and $pkeys->[0] );
    die "Too many primary keys" unless ( scalar(@$pkeys) == 1 );
    return $pkeys->[0];
}

=head2 _init_columns

Sets up the primary key columns.

=cut

sub _init_columns {
    my $self = shift;

    return if defined $self->COLUMNS;

    $self->COLUMNS( {} );

    foreach my $column_name ( @{ $self->_primary_keys } ) {
        my $column = $self->add_column($column_name);
        $column->writable(0);
        $column->readable(1);
        $column->type('serial');
        $column->mandatory(1);
    }
}

=head2 _to_record COLUMN VALUE

This B<PRIVATE> method takes a column name and a value for that column. 

It returns C<undef> unless C<COLUMN> is a valid column for this record
that refers to another record class.

If it is valid, this method returns a new record object with an id
of C<VALUE>.

=cut

sub _to_record {
    my $self        = shift;
    my $column_name = shift;
    my $value       = shift;

    my $column    = $self->column($column_name);
    my $classname = $column->refers_to();
    my $remote_column = $column->by() || 'id';

    return       unless defined $value;
    return undef unless $classname;
    return       unless UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' );

    # XXX TODO FIXME we need to figure out the right way to call new here
    # perhaps the handle should have an initiializer for records/collections
    my $object = $classname->new( $self->_handle );
    $object->load_by_cols($remote_column => $value );
    return $object;
}

sub _collection_value {
    my $self = shift;

    my $method_name = shift;
    return unless defined $method_name;

    my $column    = $self->column($method_name);
    my $classname = $column->refers_to();

    return undef unless $classname;
    return unless UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' );

    my $coll = $classname->new( handle => $self->_handle );
    $coll->limit( column =>  $column->by(), value => $self->id );
    return $coll;
}

=head2 add_column

=cut

sub add_column {
    my $self = shift;
    my $name = shift;
    $name = lc $name;
    $self->COLUMNS->{$name} = Jifty::DBI::Column->new()
        unless exists $self->COLUMNS->{$name};
    $self->COLUMNS->{$name}->name($name);
    return $self->COLUMNS->{$name};
}

=head2 column

=cut

sub column {
    my $self = shift;
    my $name = lc( shift || '' );
    return undef unless $self->COLUMNS and $self->COLUMNS->{$name};
    return $self->COLUMNS->{$name};

}

sub columns {
    my $self = shift;
    return ( sort { 
         ((($b->type||'') eq 'serial') <=> (($a->type||'') eq 'serial')) or 
            ( $a->name cmp $b->name) } values %{ $self->COLUMNS } );
}

# sub {{{ readable_attributes

=head2 readable_attributes

Returns a list this table's readable columns

=cut

sub readable_attributes {
    my $self = shift;
    return sort map { $_->name } grep { $_->readable } $self->columns;
}

=head2 writable_attributes

Returns a list of this table's writable columns


=cut

sub writable_attributes {
    my $self = shift;
    return sort map { $_->name } grep { $_->writable } $self->columns;
}

=head2 __value

Takes a column name and returns that column's value. Subclasses should
never override __value.

=cut

sub __value {
    my $self        = shift;
    my $column_name = shift;

    # If the requested column is actually an alias for another, resolve it.
    if ( $self->column($column_name)
        and defined $self->column($column_name)->alias_for_column )
    {
        $column_name = $self->column($column_name)->alias_for_column();
    }

    my $column = $self->column($column_name);

    return unless ($column);

    #Carp::confess unless ($column);

    if ( !$self->{'fetched'}{ $column->name } and my $id = $self->id() ) {
        my $pkey        = $self->_primary_key();
        my $query_string = "SELECT "
            . $column->name
            . " FROM "
            . $self->table
            . " WHERE $pkey = ?";
        my $sth = $self->_handle->simple_query( $query_string, $id );
        my ($value) = eval { $sth->fetchrow_array() };
        warn $@ if $@;

        $self->{'values'}{ $column->name }  = $value;
        $self->{'fetched'}{ $column->name } = 1;
    }
    if ( $self->{'fetched'}{ $column->name }
        && !$self->{'decoded'}{ $column->name } )
    {
        $self->_apply_output_filters(
            column    => $column,
            value_ref => \$self->{'values'}{ $column->name },
        );
        $self->{'decoded'}{ $column->name } = 1;
    }

    return $self->{'values'}{ $column->name };
}

=head2 _value

_value takes a single column name and returns that column's value for
this row.  Subclasses can override _value to insert custom access
control.

=cut

sub _value {
    my $self   = shift;
    my $column = shift;

    my $value = $self->__value( $column => @_ );
    my $method = "after_$column";
    $self->$method( \$value ) if ( $self->can($method) );
    return $value;
}

=head2 _set

_set takes a single column name and a single unquoted value.
It updates both the in-memory value of this column and the in-database copy.
Subclasses can override _set to insert custom access control.

=cut

sub _set {
    my $self = shift;
    my %args = (
        'column'          => undef,
        'value'           => undef,
        'is_sql_function' => undef,
        @_
    );

    my $method = "before_set_" . $args{column};
    $self->$method( \%args ) if ( $self->can($method) );

    return $self->__set(%args);

}

sub __set {
    my $self = shift;

    my %args = (
        'column'          => undef,
        'value'           => undef,
        'is_sql_function' => undef,
        @_
    );

    my $ret = Class::ReturnValue->new();

    my $column = $self->column( $args{'column'} );
    unless ($column) {
        $ret->as_array( 0, 'No column specified' );
        $ret->as_error(
            errno        => 5,
            do_backtrace => 0,
            message      => "No column specified"
        );
        return ( $ret->return_value );
    }

    $self->_apply_input_filters(
        column    => $column,
        value_ref => \$args{'value'}
    );

    # if value is not fetched or it's allready decoded
    # then we don't check eqality
    # we also don't call __value because it decodes value, but
    # we need encoded value
    if ( $self->{'fetched'}{ $column->name }
        || !$self->{'decoded'}{ $column->name } )
    {
        if ((      !defined $args{'value'}
                && !defined $self->{'values'}{ $column->name }
            )
            || (   defined $args{'value'}
                && defined $self->{'values'}{ $column->name }
                && $args{'value'} eq $self->{'values'}{ $column->name } )
            )
        {
            $ret->as_array( 1, "That is already the current value" );
            return ( $ret->return_value );
        }
    }

    my $method = "validate_" . $column->name;
    my ($ok, $msg) = $self->$method( $args{'value'} );
    unless ( $ok ) {
        $ret->as_array( 0, 'Illegal value for ' . $column->name );
        $ret->as_error(
            errno        => 3,
            do_backtrace => 0,
            message      => "Illegal value for " . $column->name
        );
        return ( $ret->return_value );
    }

    # The blob handling will destroy $args{'Value'}. But we assign
    # that back to the object at the end. this works around that
    my $unmunged_value = $args{'value'};

    unless ( $self->_handle->knows_blobs ) {

        # Support for databases which don't deal with LOBs automatically
        if ( $column->type =~ /^(text|longtext|clob|blob|lob)$/i ) {
            my $bhash
                = $self->_handle->blob_params( $column->name, $column->type );
            $bhash->{'value'} = $args{'value'};
            $args{'value'} = $bhash;
        }
    }

    my $val = $self->_handle->update_record_value(
        %args,
        table        => $self->table(),
        primary_keys => { $self->primary_keys() }

    );
    unless ($val) {
        my $message
            = $column->name . " could not be set to " . $args{'value'} . ".";
        $ret->as_array( 0, $message );
        $ret->as_error(
            errno        => 4,
            do_backtrace => 0,
            message      => $message
        );
        return ( $ret->return_value );
    }

    # If we've performed some sort of "functional update"
    # then we need to reload the object from the DB to know what's
    # really going on. (ex SET Cost = Cost+5)
    if ( $args{'is_sql_function'} ) {

        # XXX TODO primary_keys
        $self->load_by_cols( id => $self->id );
    }
    else {
        $self->{'values'}->{ $column->name } = $unmunged_value;
        $self->{'decoded'}{ $column->name } = 0;
    }
    $ret->as_array( 1, "The new value has been set." );
    return ( $ret->return_value );
}

=head2 _Validate column VALUE

Validate that value will be an acceptable value for column. 

Currently, this routine does nothing whatsoever. 

If it succeeds (which is always the case right now), returns true. Otherwise returns false.

=cut

sub _validate {
    my $self  = shift;
    my $column = shift;
    my $value = shift;

 #Check type of input
 #If it's null, are nulls permitted?
 #If it's an int, check the # of bits
 #If it's a string,
 #check length
 #check for nonprintables
 #If it's a blob, check for length
 #In an ideal world, if this is a link to another table, check the dependency.
    return (1);
}

=head2 load

Takes a single argument, $id. Calls load_by_cols to retrieve the row whose primary key
is $id

=cut

sub load {
    my $self = shift;

    return unless @_ and defined $_[0];

    return $self->load_by_cols( id => shift );
}

=head2 load_by_cols

Takes a hash of columns and values. Loads the first record that matches all
keys.

The hash's keys are the columns to look at.

The hash's values are either: scalar values to look for
OR has references which contain 'operator' and 'value'

=cut

sub load_by_cols {
    my $self = shift;
    my %hash = (@_);
    my ( @bind, @phrases );
    foreach my $key ( keys %hash ) {
        if ( defined $hash{$key} && $hash{$key} ne '' ) {
            my $op;
            my $value;
            my $function = "?";
            if ( ref $hash{$key} eq 'HASH' ) {
                $op       = $hash{$key}->{operator};
                $value    = $hash{$key}->{value};
                $function = $hash{$key}->{function} || "?";
            }
            else {
                $op    = '=';
                $value = $hash{$key};
            }

            push @phrases, "$key $op $function";
            push @bind,    $value;
        }
        else {
            push @phrases, "($key IS NULL OR $key = ?)";
            my $column = $self->column($key);

            if ( $column->is_numeric ) {
                push @bind, 0;
            }
            else {
                push @bind, '';
            }

        }
    }

    my $query_string = "SELECT  * FROM "
        . $self->table
        . " WHERE "
        . join( ' AND ', @phrases );
    return ( $self->_load_from_sql( $query_string, @bind ) );
}

=head2 load_by_primary_keys 


=cut

sub load_by_primary_keys {
    my $self = shift;
    my $data = ( ref $_[0] eq 'HASH' ) ? $_[0] : {@_};

    my %cols = ();
    foreach ( @{ $self->_primary_keys } ) {
        return ( 0, "Missing PK column: '$_'" ) unless defined $data->{$_};
        $cols{$_} = $data->{$_};
    }
    return ( $self->load_by_cols(%cols) );
}

=head2 load_from_hash

Takes a hashref, such as created by Jifty::DBI and populates this record's
loaded values hash.

=cut

sub load_from_hash {
    my $self    = shift;
    my $hashref = shift;

    foreach my $f ( keys %$hashref ) {
        $self->{'fetched'}{ lc $f } = 1;
    }

    $self->{'values'}  = $hashref;
    $self->{'decoded'} = {};
    return $self->id();
}

=head2 _load_from_sql QUERYSTRING @BIND_VALUES

Load a record as the result of an SQL statement

=cut

sub _load_from_sql {
    my $self        = shift;
    my $query_string = shift;
    my @bind_values = (@_);

    my $sth = $self->_handle->simple_query( $query_string, @bind_values );

    #TODO this only gets the first row. we should check if there are more.

    return ( 0, "Couldn't execute query" ) unless $sth;

    $self->{'values'}  = $sth->fetchrow_hashref;
    $self->{'fetched'} = {};
    $self->{'decoded'} = {};
    if ( !$self->{'values'} && $sth->err ) {
        return ( 0, "Couldn't fetch row: " . $sth->err );
    }

    unless ( $self->{'values'} ) {
        return ( 0, "Couldn't find row" );
    }

    ## I guess to be consistant with the old code, make sure the primary
    ## keys exist.

    if ( grep { not defined } $self->primary_keys ) {
        return ( 0, "Missing a primary key?" );
    }

    foreach my $f ( keys %{ $self->{'values'} } ) {
        $self->{'fetched'}{ lc $f } = 1;
    }
    return ( 1, "Found Object" );

}

=head2 create

Takes an array of key-value pairs and drops any keys that aren't known
as columns for this recordtype

=cut 

sub create {
    my $self    = shift;
    my %attribs = @_;

    $self->before_create( \%attribs ) if $self->can('before_create');

    foreach my $column_name ( keys %attribs ) {
        my $column = $self->column($column_name);
        unless ($column) {
            Carp::confess "$column_name isn't a column we know about";
        }
        if (    $column->readable
            and $column->refers_to
            and UNIVERSAL::isa( $column->refers_to, "Jifty::DBI::Record" ) )
        {
            $attribs{$column_name} = $attribs{$column_name}->id
                if UNIVERSAL::isa( $attribs{$column_name},
                'Jifty::DBI::Record' );
        }

        $self->_apply_input_filters(
            column    => $column,
            value_ref => \$attribs{$column_name},
        );

    }
    unless ( $self->_handle->knows_blobs ) {

        # Support for databases which don't deal with LOBs automatically
        foreach my $column_name ( keys %attribs ) {
            my $column = $self->column($column_name);
            if ( $column->type =~ /^(text|longtext|clob|blob|lob)$/i ) {
                my $bhash = $self->_handle->blob_params( $column_name,
                    $column->type );
                $bhash->{'value'} = $attribs{$column_name};
                $attribs{$column_name} = $bhash;
            }
        }
    }
    my $ret = $self->_handle->insert( $self->table, %attribs );
    $self->after_create($ret) if $self->can('after_create');
    return ($ret);
}

=head2 delete

Delete this record from the database. On failure return a
Class::ReturnValue with the error. On success, return 1;

=cut

sub delete {
    my $self = shift;
    $self->before_delete() if $self->can('before_delete');
    my $ret = $self->__delete;
    $self->after_delete( \$ret ) if $self->can('after_delete');
    return ($ret);

}

sub __delete {
    my $self = shift;

    #TODO Check to make sure the key's not already listed.
    #TODO Update internal data structure

    ## Constructs the where clause.
    my @bind  = ();
    my %pkeys = $self->primary_keys();
    my $where = 'WHERE ';
    foreach my $key ( keys %pkeys ) {
        $where .= $key . "=?" . " AND ";
        push( @bind, $pkeys{$key} );
    }

    $where =~ s/AND\s$//;
    my $query_string = "DELETE FROM " . $self->table . ' ' . $where;
    my $return      = $self->_handle->simple_query( $query_string, @bind );

    if ( UNIVERSAL::isa( 'Class::ReturnValue', $return ) ) {
        return ($return);
    }
    else {
        return (1);
    }
}

=head2 table

This method returns this class's default table name. It uses
Lingua::EN::Inflect to pluralize the class's name as we believe that
class names for records should be in the singular and table names
should be plural.

If your class name is C<My::App::Rhino>, your table name will default
to C<rhinos>. If your class name is C<My::App::RhinoOctopus>, your
default table name will be C<rhino_octopuses>. Not perfect, but
arguably correct.

=cut

sub table {
    my $self = shift;

    if ( not ref($self) ) {
        return $self->_guess_table_name();
    }
    $self->{__table_name} ||= $self->_guess_table_name;

    return $self->{__table_name};
}

=head2 _guess_table_name

Guesses a table name based on the class's last part.


=cut

sub _guess_table_name {
    my $self = shift;
    my $class = ref($self) ? ref($self) : $self;
    die "Couldn't turn " . $class . " into a table name"
        unless ( $class =~ /(?:\:\:)?(\w+)$/ );
    my $table = $1;
    $table =~ s/(?<=[a-z])([A-Z]+)/"_" . lc($1)/eg;
    $table =~ tr/A-Z/a-z/;
    $table = Lingua::EN::Inflect::PL_N($table);
    return ($table);

}

=head2 _handle

Returns or sets the current Jifty::DBI::Handle object

=cut

sub _handle {
    my $self = shift;
    if (@_) {
        $self->{'DBIxHandle'} = shift;
    }
    return ( $self->{'DBIxHandle'} );
}

=for private refers_to

used for the declarative syntax


=cut

sub refers_to (@) {
    my $class = shift;
    my (%args) = @_;

    return ( refers_to => $class, %args );
}

sub _filters {
    my $self = shift;
    my %args = ( direction => 'input', column => undef, @_ );

    my @filters = ();
    my @objs = ( $self, $args{'column'}, $self->_handle );
    @objs = reverse @objs if $args{'direction'} eq 'output';
    my $method = $args{'direction'} . "_filters";
    foreach my $obj (@objs) {
        push @filters, $obj->$method();
    }
    return grep $_, @filters;
}

sub _apply_input_filters {
    return (shift)->_apply_filters( direction => 'input', @_ );
}

sub _apply_output_filters {
    return (shift)->_apply_filters( direction => 'output', @_ );
}

sub _apply_filters {
    my $self = shift;
    my %args = (
        direction => 'input',
        column    => undef,
        value_ref => undef,
        @_
    );

    my @filters = $self->_filters(%args);
    my $action = $args{'direction'} eq 'output' ? 'decode' : 'encode';
    foreach my $filter_class (@filters) {
        local $UNIVERSAL::require::ERROR;
        $filter_class->require();
        if ($UNIVERSAL::require::ERROR) {
            warn $UNIVERSAL::require::ERROR;
            next;
        }
        my $filter = $filter_class->new(
            record    => $self,
            column    => $args{'column'},
            value_ref => $args{'value_ref'},
        );

        # XXX TODO error proof this
        $filter->$action();
    }
}

1;

__END__



=head1 AUTHOR

Jesse Vincent, <jesse@fsck.com> 

Enhancements by Ivan Kohler, <ivan-rt@420.am>

Docs by Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

L<Jifty::DBI>

=cut


