package Jifty::DBI::Record;

use strict;
use warnings;

use vars qw($AUTOLOAD);
use Class::ReturnValue;
use Lingua::EN::Inflect;
use Jifty::DBI::Column;

use base qw/Class::Data::Inheritable/;

Jifty::DBI::Record->mk_classdata('COLUMNS'); 

#our $COLUMNS; # The global cache of all schema columns

=head1 NAME

Jifty::DBI::Record - Superclass for records loaded by Jifty::DBI::Collection

=head1 SYNOPSIS

  package MyRecord;
  use base qw/Jifty::DBI::Record/;
  
=head1 DESCRIPTION

Jifty::DBI::Record encapuslates records and tables as part of the L<Jifty::DBI> 
object-relational mapper.


=head2 What is it trying to do. 

Jifty::DBI::Record abstracts the agony of writing the common and generally 
simple SQL statements needed to serialize and De-serialize an object to the
database.  In a traditional system, you would define various methods on 
your object 'create', 'find', 'modify', and 'delete' being the most common. 
In each method you would have a SQL statement like: 

  select * from table where value='blah';

If you wanted to control what data a user could modify, you would have to 
do some special magic to make accessors do the right thing. Etc.  The 
problem with this approach is that in a majority of the cases, the SQL is 
incredibly simple and the code from one method/object to the next was 
basically the same.  

<trumpets>

Enter, Jifty::DBI::Record. 

With::Record, you can in the simple case, remove all of that code and 
replace it by defining two methods and inheriting some code.  Its pretty 
simple, and incredibly powerful.  For more complex cases, you can, gasp, 
do more complicated things by overriding certain methods.  Lets stick with
the simple case for now. 



=head2 An Annotated Example

The example code below makes the following assumptions: 

=over 4

=item *

The database is 'postgres',

=item *

The host is 'reason',

=item *

The login name is 'mhat',

=item *

The database is called 'example', 

=item *

The table is called 'simple', 

=item *

The table looks like so: 

      id     integer     not NULL,   primary_key(id),
      foo    varchar(10),
      bar    varchar(10)

=back

First, let's define our record class in a new module named "Simple.pm".

  000: package Simple; 
  001: use Jifty::DBI::Record;
  002: @ISA = (Jifty::DBI::Record);

This should be pretty obvious, name the package, import ::Record and then 
define ourself as a subclass of ::Record. 

  013: 
  014: sub schema {
  015:   {  
  016:     Foo => { 'read'  => 1 },
  017:     Bar => { 'read'  => 1, 'write' => 1  },
  018:     Id  => { 'read'  => 1 }
  019:   };
  020: }

  XXX TODO add types

What's happening might be obvious, but just in case this method is going to 
return a reference to a hash. That hash is where our columns are defined, 
as well as what type of operations are acceptable.  

  021: 
  022: 1;             

Like all perl modules, this needs to end with a true value. 

Now, on to the code that will actually *do* something with this object. 
This code would be placed in your Perl script.

  000: use Jifty::DBI::Handle;
  001: use Simple;

Use two packages, the first is where I get the DB handle from, the latter 
is the object I just created. 

  002: 
  003: my $handle = Jifty::DBI::Handle->new();
  004:    $handle->Connect( 'Driver'   => 'Pg',
  005: 		          'Database' => 'test', 
  006: 		          'Host'     => 'reason',
  007: 		          'User'     => 'mhat',
  008: 		          'Password' => '');

Creates a new Jifty::DBI::Handle, and then connects to the database using 
that handle.  Pretty straight forward, the password '' is what I use 
when there is no password.  I could probably leave it blank, but I find 
it to be more clear to define it.

  009: 
  010: my $s = Simple->new($handle);
  011: 
  012: $s->load_by_id(1); 

load_by_id is one of four 'load_by_*' methods, as the name suggests it
searches for an row in the database that has id='0'.  This causes,
what I think is a bug, in that it current requires there to be an id
field. More reasonably it also assumes that the id field is
unique. load_by_id($id) will do undefined things if there is >1 row
with the same id.

In addition to load_by_id, we also have:

=over 4

=item load_by_col 

Takes two arguments, a column name and a value.  Again, it will do 
undefined things if you use non-unique things.  

=item load_by_cols

Takes a hash of columns=>values and returns the *first* to match. 
First is probably lossy across databases vendors. 

=item LoadFromHash

Populates this record with data from a Jifty::DBI.  I'm 
currently assuming that Jifty::DBI is what we use in 
cases where we expect > 1 record.  More on this later.

=back

Now that we have a populated object, we should do something with it! ::Record
automagically generates accessos and mutators for us, so all we need to do 
is call the methods.  accessors are named <Field>(), and Mutators are named 
Set<Field>($).  On to the example, just appending this to the code from 
the last example.

  013:
  014: print "ID  : ", $s->Id(),  "\n";
  015: print "Foo : ", $s->Foo(), "\n";
  016: print "Bar : ", $s->Bar(), "\n";

Thats all you have to to get the data, now to change the data!

  017:
  018: $s->SetBar('NewBar');

Pretty simple! Thats really all there is to it.  Set<Field>($) returns 
a boolean and a string describing the problem.  Lets look at an example of
what will happen if we try to set a 'Id' which we previously defined as 
read only. 

  019: my ($res, $str) = $s->SetId('2');
  020: if (! $res) {
  021:   ## Print the error!
  022:   print "$str\n";
  023: } 

The output will be:

  >> Immutable field

Currently Set<Field> updates the data in the database as soon as you call
it.  In the future I hope to extend ::Record to better support transactional
operations, such that updates will only happen when "you" say so.

Finally, adding a removing records from the database.  ::Record provides a 
Create method which simply takes a hash of key=>value pairs.  The keys 
exactly	map to database fields. 

  023: ## Get a new record object.
  024: $s1 = Simple->new($handle);
  025: $s1->Create('Id'  => 4,
  026: 	           'Foo' => 'Foooooo', 
  027: 	           'Bar' => 'Barrrrr');

Poof! A new row in the database has been created!  Now lets delete the 
object! 

  028:
  029: $s1 = undef;
  030: $s1 = Simple->new($handle);
  031: $s1->load_by_id(4);
  032: $s1->Delete();

And its gone. 

For simple use, thats more or less all there is to it.  In the future, I hope to exapand 
this HowTo to discuss using container classes,  overloading, and what 
ever else I think of.

=head1 METHODS

=head2  new 

Instantiate a new record object.

=cut

sub new {
    my $proto = shift;

    my $class = ref($proto) || $proto;
    my $self = {};
    bless( $self, $class );

    $self->_init_columns() unless $self->COLUMNS;

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

            no strict 'refs'; # We're going to be defining subs
    if ( $action eq 'read' and $column->readable ) {

        if ( $column->refers_to_record_class ) {
            *{$AUTOLOAD}
                = sub { $_[0]->_to_record( $column_name, $_[0]->__value($column_name) ) };
        }
        elsif ( $column->refers_to_collection_class ) {
            *{$AUTOLOAD} = sub { $_[0]->_collection_value($column_name) };
        }
        else {
            *{$AUTOLOAD} = sub { return ( $_[0]->_value($column_name) ) };
        }
        goto &$AUTOLOAD;
    }

    if ( $action eq 'write' ) {
        if ( $column->writable ) {

        if ( $column->refers_to_record_class ) {
            *{$AUTOLOAD} = sub {
                my $self = shift;
                my $val  = shift;

                $val = $val->id
                    if UNIVERSAL::isa( $val, 'Jifty::DBI::Record' );
                return ( $self->_set( column => $column_name, value => $val ) );
            };
        }
        else {
            *{$AUTOLOAD} = sub {
                return ( $_[0]->_set( column => $column_name, value => $_[1] ) );
            };
        }
        goto &$AUTOLOAD;
    } else {
            return (0, 'Immutable field');
    }
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
    my $self = shift;
    my $method = shift;

    my ($column_name, $action);

    if ( $method =~ /^.*::set_(\w+)$/o ) {
        $column_name = $1;
        $action = 'write';
    }
    elsif ( $method =~ /^.*::validate_(\w+)$/o ) {
        $column_name = $1;
        $action = 'validate';


    }
    elsif ( $method =~ /^.*::(\w+)$/o) {
        $column_name = $1;
        $action = 'read';

    }
    return ($column_name, $action);

}
=head2 _accessible COLUMN ATTRIBUTE

Private method. 

DEPRECATED

Returns undef unless C<COLUMN> has a true value for C<ATTRIBUTE>.

Otherwise returns C<COLUMN>'s value for that attribute.


=cut

sub _accessible {
    my $self = shift;
    my $column_name = shift;
    my $attribute = lc( shift || '' );

    my $col = $self->column($column_name);
    return undef unless ($col and $col->can($attribute));
    return $col->$attribute();

}

=head2 _primary_keys

Return our primary keys. (Subclasses should override this, but our default is that we have one primary key, named 'id'.)

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

Turns your sub schema into a set of column objects
=cut

sub _init_columns {
    my $self = shift;

    $self->COLUMNS({}); # Clear out the columns hash

    foreach my $column_name ( @{$self->_primary_keys} ) {
        my $column = $self->add_column($column_name);
        $column->writable(0);
    }

    my $schema = $self->schema;

    for my $column_name ( keys %$schema ) {
        my $column = $self->add_column($column_name);
        # Default, everything readable and writable
        $column->readable(1);

        if ( $schema->{$column_name}{'read'} ) {
            $column->readable( $schema->{$column_name}{'read'});
        } else {
            $column->readable(1);
        }
    
        if ( $schema->{$column_name}{'write'} ) {
            $column->writable( $schema->{$column_name}{'write'});
        } elsif (not defined $column->writable) { # don't want to make pkeys writable
            $column->writable(1);
        }

    
        # Next time, all-lower hash keys
        my $type = $schema->{$column_name}{'type'} || $schema->{$column_name}{'TYPE'};

        if ($type) {
            $column->type($type);

        }

        my $refclass = $schema->{$column_name}{'REFERENCES'} || $schema->{$column_name}{'references'};

        if ($refclass) {
            if ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Record' ) ) {
                if ( $column_name =~ /(.*)_id$/ ) {

                    my $virtual_column = $self->add_column($1);
                    $virtual_column->refers_to_record_class($refclass);
                    $virtual_column->alias_for_column($column_name);
                    $virtual_column->readable( $schema->{$column_name}{'read'} || 1);
                }
                else {
                    $column->refers_to_record_class($refclass);
                }

            }
            elsif ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Collection' ) ) {
                $column->refers_to_collection_class($refclass);
            }
            else {
                warn "Error: $refclass neither Record nor Collection";
            }
        }
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
    my $self  = shift;
    my $column_name = shift;
    my $value = shift;



    my $column = $self->column($column_name);
    my $classname = $column->refers_to_record_class();


    return unless defined $value;
    return undef unless $classname;

    return unless UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' );

    # XXX TODO FIXME we need to figure out the right way to call new here
    # perhaps the handle should have an initiializer for records/collections

    my $object = $classname->new( $self->_handle );
    $object->load_by_id($value);
    return $object;
}

sub _collection_value {
    my $self = shift;

    my $method_name = shift;
    return unless defined $method_name;

    my $schema      = $self->schema;
    my $description = $schema->{$method_name};
    return unless $description;

    my $classname = $description->{'REFERENCES'};

    return unless UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' );

    my $coll = $classname->new( handle => $self->_handle );

    $coll->Limit( FIELD => $description->{'KEY'}, VALUE => $self->id );

    return $coll;
}

=head2 add_column

=cut

sub add_column {
    my $self = shift;
    my $name = shift;
    $name = lc $name;
    $self->COLUMNS->{$name} = Jifty::DBI::Column->new() unless exists $self->COLUMNS->{$name};
    $self->COLUMNS->{$name}->name($name);
    return $self->COLUMNS->{$name};
}


=head2 column

=cut

sub column {
    my $self = shift;
    my $name = shift;
    $name = lc $name;
    return undef unless $self->COLUMNS and $self->COLUMNS->{$name};
    return $self->COLUMNS->{$name} ;

}

sub columns {
    my $self = shift;
    return (values %{$self->COLUMNS});
}


# sub {{{ readable_attributes

=head2 readable_attributes

Returns a list this table's readable columns

=cut

sub readable_attributes {
    my $self     = shift;
    return sort map {$_->name }  grep { $_->readable } $self->columns;
}

=head2 writable_attributes

Returns a list of this table's writable columns


=cut

sub writable_attributes {
    my $self = shift;
    return sort map { $_->name } grep { $_->writable } $self->columns;
}

=head2 __value

Takes a field name and returns that field's value. Subclasses should
never override __value.

=cut

sub __value {
    my $self  = shift;
    my $field = lc shift;

    Carp::confess unless ($field);
    # If the requested column is actually an alias for another, resolve it.
    while ( $self->column($field) and defined $self->column($field)->alias_for_column) {
        warn "Turning $field into ". $self->column($field)->alias_for_column() ;
        $field = $self->column($field)->alias_for_column() 
    }

    warn "Now field is $field\n";
    if ( !$self->{'fetched'}{$field} and my $id = $self->id() ) {
        my $pkey = $self->_primary_key();
        my $QueryString
            = "SELECT $field FROM " . $self->table . " WHERE $pkey = ?";
        my $sth = $self->_handle->simple_query( $QueryString, $id );
        my ($value) = eval { $sth->fetchrow_array() };
        warn $@ if $@;

        $self->{'values'}{$field}  = $value;
        $self->{'fetched'}{$field} = 1;
    }

    return $self->{'values'}{$field};
}

=head2 _value

_value takes a single column name and returns that column's value for
this row.  Subclasses can override _value to insert custom access
control.

=cut

sub _value {
    my $self = shift;
    return ( $self->__value(@_) );
}

=head2 _set

_set takes a single column name and a single unquoted value.
It updates both the in-memory value of this column and the in-database copy.
Subclasses can override _set to insert custom access control.

=cut

sub _set {
    my $self = shift;
    return ( $self->__set(@_) );
}

sub __set {
    my $self = shift;

    my %args = (
        'column'  => undef,
        'value'  => undef,
        'is_sql_function' => undef,
        @_
    );

    if ($args{'field'} ) {
        Carp::cluck("field in ->set is deprecated");
            $args{'column'}          = delete $args{'field'};
    }
    if ($args{'is_sql'}) {
        Carp::cluck("is_sql in ->set is deprecated");
        $args{'is_sql_function'} = delete $args{'is_sql'};
    }
    my $ret = Class::ReturnValue->new();

    my $column = $self->column(lc $args{'column'});
    unless ( $column) {
        $ret->as_array( 0, 'No column specified' );
        $ret->as_error(
            errno        => 5,
            do_backtrace => 0,
            message      => "No column specified"
        );
        return ( $ret->return_value );
    }
    if ( !defined( $args{'value'} ) ) {
        $ret->as_array( 0, "No value passed to _set" );
        $ret->as_error(
            errno        => 2,
            do_backtrace => 0,
            message      => "No value passed to _set"
        );
        return ( $ret->return_value );
    }
    elsif ( ( defined $self->__value($column->name) )
        and ( $args{'value'} eq $self->__value($column->name) ) )
    {
        $ret->as_array( 0, "That is already the current value" );
        $ret->as_error(
            errno        => 1,
            do_backtrace => 0,
            message      => "That is already the current value"
        );
        return ( $ret->return_value );
    }

    # First, we truncate the value, if we need to.
    

    $args{'value'} = $self->truncate_value( $column->name, $args{'value'} );

    my $method = "validate_" . $column->name;
    unless ( $self->$method( $args{'value'} ) ) {
        $ret->as_array( 0, 'Illegal value for ' . $column->name);
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
            my $bhash = $self->_handle->blob_params( $column->name, $column->type );
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
        my $message = $column->name . " could not be set to " . $args{'value'} . ".";
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
        $self->load_by_id( $self->id );
    }
    else {
        $self->{'values'}->{$column->name} = $unmunged_value;
    }
    $ret->as_array( 1, "The new value has been set." );
    return ( $ret->return_value );
}

=head2 _canonicalize PARAMHASH

This routine massages an input value (VALUE) for FIELD into something that's 
going to be acceptable.

Takes

=over

=item FIELD

=item VALUE

=item FUNCTION

=back


Takes:

=over

=item FIELD

=item VALUE

=item FUNCTION

=back

Returns a replacement VALUE. 

=cut

sub _canonicalize {
    my $self  = shift;
    my $field = shift;

}

=head2 _Validate FIELD VALUE

Validate that VALUE will be an acceptable value for FIELD. 

Currently, this routine does nothing whatsoever. 

If it succeeds (which is always the case right now), returns true. Otherwise returns false.

=cut

sub _validate {
    my $self  = shift;
    my $field = shift;
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

=head2 truncate_value  COLUMN VALUE

Truncate a value that's about to be set so that it will fit inside the
database' s idea of how big the column is.

(Actually, it looks at L<Jifty::DBI>'s concept of the database, not
directly into the db).

=cut

sub truncate_value {
    my $self  = shift;
    my $column_name   = shift;
    my $value = shift;

    # We don't need to truncate empty things.
    return undef unless ( defined($value) ); 

    my $column = $self->column($column_name);

    die "No column $column_name" unless ($column);

    my $truncate_to;
    if ( $column->length && !$column->is_numeric ) {
        $truncate_to = $column->length;
    }
    elsif ( $column->type && $column->type =~ /char\((\d+)\)/ ) {
        $truncate_to = $1;
    }

    return ($value) unless ($truncate_to);    # don't need to truncate

    # Perl 5.6 didn't speak unicode
    return substr( $value, 0, $truncate_to ) unless ( $] >= 5.007 );

    require Encode;

    if ( Encode::is_utf8($value) ) {
        return Encode::decode(
            utf8 =>
                substr( Encode::encode( utf8 => $value ), 0, $truncate_to ),
            Encode::FB_QUIET(),
        );
    }
    else {
        return Encode::encode(
            utf8 => Encode::decode(
                utf8 => substr( $value, 0, $truncate_to ),
                Encode::FB_QUIET(),
            )
        );

    }

}

# load should do a bit of overloading
# if we call it with only one argument, we're trying to load by reference.
# if we call it with a passel of arguments, we're trying to load by value
# The latter is primarily important when we've got a whole set of record that we're
# reading in with a recordset class and want to instantiate objefcts for each record.

=head2 load

Takes a single argument, $id. Calls load_by_id to retrieve the row whose primary key
is $id

=cut

sub load {
    my $self = shift;

    return $self->load_by_id(@_);
}

=head2 load_by_col

Takes two arguments, a column and a value. The column can be any table column
which contains unique values.  Behavior when using a non-unique value is
undefined

=cut

sub load_by_col {
    my $self = shift;
    my $col  = shift;
    my $val  = shift;

    return ( $self->load_by_cols( $col => $val ) );
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

            if (   $column->is_numeric)
            {
                push @bind, 0;
            }
            else {
                push @bind, '';
            }

        }
    }

    my $QueryString = "SELECT  * FROM "
        . $self->table
        . " WHERE "
        . join( ' AND ', @phrases );
    return ( $self->_load_from_sql( $QueryString, @bind ) );
}

=head2 load_by_id

Loads a record by its primary key. Your record class must define a single primary key column.

=cut

sub load_by_id {
    my $self = shift;
    my $id   = shift;

    $id = 0 if ( !defined($id) );
    my $pkey = $self->_primary_key();
    return ( $self->load_by_cols( $pkey => $id ) );
}

=head2 load_by_primary_keys 

Like load_by_id with basic support for compound primary keys.

=cut

sub load_by_primary_keys {
    my $self = shift;
    my $data = ( ref $_[0] eq 'HASH' ) ? $_[0] : {@_};

    my %cols = ();
    foreach ( @{ $self->_primary_keys } ) {
        return ( 0, "Missing PK field: '$_'" ) unless defined $data->{$_};
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

    $self->{'values'} = $hashref;
    return $self->id();
}

=head2 _load_from_sql QUERYSTRING @BIND_VALUES

Load a record as the result of an SQL statement

=cut

sub _load_from_sql {
    my $self        = shift;
    my $QueryString = shift;
    my @bind_values = (@_);

    my $sth = $self->_handle->simple_query( $QueryString, @bind_values );

    #TODO this only gets the first row. we should check if there are more.

    return ( 0, "Couldn't execute query" ) unless $sth;

    $self->{'values'}  = $sth->fetchrow_hashref;
    $self->{'fetched'} = {};
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

    foreach my $column_name ( keys %attribs ) {
        my $column = $self->column($column_name);
        unless ($column) {
            die "$column_name isn't a column we know about"
        }
        if ( $column->readable and $column->refers_to_record_class ) {
            $attribs{$column_name} = $attribs{$column_name}->id
                if UNIVERSAL::isa( $attribs{$column_name}, 'Jifty::DBI::Record' );
        }

        #Truncate things that are too long for their datatypes
        $attribs{$column_name} = $self->truncate_value( $column_name => $attribs{$column_name} );

    }
    unless ( $self->_handle->knows_blobs ) {
        # Support for databases which don't deal with LOBs automatically
        foreach my $column_name ( keys %attribs ) {
            my $column = $self->column($column_name);
            if ( $column->type =~ /^(text|longtext|clob|blob|lob)$/i )
            {
                my $bhash = $self->_handle->blob_params( $column_name,
                    $column->type );
                $bhash->{'value'} = $attribs{$column_name};
                $attribs{$column_name} = $bhash;
            }
        }
    }
    return ( $self->_handle->insert( $self->table, %attribs ) );
}

=head2 delete

Delete this record from the database. On failure return a
Class::ReturnValue with the error. On success, return 1;

=cut

sub delete {
    $_[0]->__delete;
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
    my $QueryString = "DELETE FROM " . $self->table . ' ' . $where;
    my $return      = $self->_handle->simple_query( $QueryString, @bind );

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
    use Carp; die Carp::longmess unless ref $self;

    if (not $self->{__table_name} ) {
	    my $class = ref($self);
	    die "Couldn't turn ".$class." into a table name" unless ($class =~ /::(\w+)$/);
            my $table = $1;
            $table =~ s/(?<=[a-z])([A-Z]+)/"_" . lc($1)/eg;
            $table =~ tr/A-Z/a-z/;
            $table = Lingua::EN::Inflect::PL_N($table);
	    $self->{__table_name} = $table;
    }
    return $self->{__table_name};
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

=head2 schema

You must subclass schema to return your table's columns.

XXX: See L<Jifty::DBI::SchemaGenerator> (I bet)

=cut

# This stub is here to prevent a call to AUTOLOAD
sub schema {}


1;

__END__



=head1 AUTHOR

Jesse Vincent, <jesse@fsck.com> 

Enhancements by Ivan Kohler, <ivan-rt@420.am>

Docs by Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

L<Jifty::DBI>

=cut


