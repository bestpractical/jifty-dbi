#$Header/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Record.pm,v 1.21 2001/02/28 21:36:27 jesse Exp $
package Jifty::DBI::Record;

use strict;
use warnings;

use vars qw($AUTOLOAD);
use Class::ReturnValue;

=head1 NAME

Jifty::DBI::Record - Superclass for records loaded by SearchBuilder

=head1 SYNOPSIS

  package MyRecord;
  use base qw/Jifty::DBI::Record/;
  
  sub _Init {
      my $self       = shift;
      my $DBIxHandle =
	shift;    # A Jifty::DBI::Handle::foo object for your database
  
      $self->_handle($DBIxHandle);
      $self->table("Users");
  }
  
  # Tell Record what the primary keys are
  sub _primary_keys {
      return ['id'];
  }
  
  # Preferred and most efficient way to specify fields attributes in a derived
  # class, used by the autoloader to construct Attrib and SetAttrib methods.

  # read: calling $Object->Foo will return the value of this record's Foo column  
  # write: calling $Object->SetFoo with a single value will set Foo's value in
  #        both the loaded object and the database  
  sub _class_accessible {
      {
	  Tofu => { 'read' => 1, 'write' => 1 },
	  Maz  => { 'auto' => 1, },
	  Roo => { 'read' => 1, 'auto' => 1, 'public' => 1, },
      };
  }
  
  # A subroutine to check a user's password without returning the current value
  # For security purposes, we didn't expose the Password method above
  sub IsPassword {
      my $self = shift;
      my $try  = shift;
  
      # note two __s in __value.  Subclasses may muck with _value, but
      # they should never touch __value
  
      if ( $try eq $self->__value('Password') ) {
	  return (1);
      }
      else {
	  return (undef);
      }
  }
  
  # Override Jifty::DBI::Create to do some checking on create
  sub Create {
      my $self   = shift;
      my %fields = (
	  UserId   => undef,
	  Password => 'default',    #Set a default password
	  @_
      );
  
      # Make sure a userid is specified
      unless ( $fields{'UserId'} ) {
	  die "No userid specified.";
      }
  
      # Get Jifty::DBI::Record->Create to do the real work
      return (
	  $self->SUPER::Create(
	      UserId   => $fields{'UserId'},
	      Password => $fields{'Password'},
	      Created  => time
	  )
      );
  }

=head1 DESCRIPTION

Jifty::DBI::Record is designed to work with Jifty::DBI.


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

The two methods in question are '_Init' and '_class_accessible', all they 
really do are define some values and send you on your way.  As you might 
have guessed the '_' suggests that these are private methods, they are. 
They will get called by your record objects constructor.  

=over 4

=item '_Init' 

Defines what table we are talking about, and set a variable to store 
the database handle. 

=item '_class_accessible

Defines what operations may be performed on various data selected 
from the database.  For example you can define fields to be mutable,
or immutable, there are a few other options but I don't understand 
what they do at this time. 

=back

And really, thats it.  So lets have some sample code.

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

  003: 
  004: sub _Init {
  005:   my $this   = shift; 
  006:   my $handle = shift;
  007: 
  008:   $this->_handle($handle); 
  009:   $this->table("Simple"); 
  010:   
  011:   return ($this);
  012: }

Here we set our handle and table name, while its not obvious so far, we'll 
see later that $handle (line: 006) gets passed via ::Record::new when a 
new instance is created.  Thats actually an important concept, the DB handle 
is not bound to a single object but rather, its shared across objects. 

  013: 
  014: sub _class_accessible {
  015:   {  
  016:     Foo => { 'read'  => 1 },
  017:     Bar => { 'read'  => 1, 'write' => 1  },
  018:     Id  => { 'read'  => 1 }
  019:   };
  020: }

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

load_by_id is one of four 'LoadBy' methods,  as the name suggests it searches
for an row in the database that has id='0'.  ::SearchBuilder has, what I 
think is a bug, in that it current requires there to be an id field. More 
reasonably it also assumes that the id field is unique. load_by_id($id) will 
do undefined things if there is >1 row with the same id.  

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
is call the methods.  Accessors are named <Field>(), and Mutators are named 
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

=head1 METHOD NAMING
 
Each method has a lower case alias; '_' is used to separate words.
For example, the method C<_primary_keys> has the alias C<_primary_keys>.

=head1 METHODS

=cut

=head2  new 

Instantiate a new record object.

=cut

sub new {
    my $proto = shift;

    my $class = ref($proto) || $proto;
    my $self = {};
    bless( $self, $class );
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

    no strict 'refs';
    my ($Attrib) = ( $AUTOLOAD =~ /::(\w+)$/o );

    if ( $self->_accessible( $Attrib, 'read' ) ) {
        *{$AUTOLOAD} = sub { return ( $_[0]->_value($Attrib) ) };
        goto &$AUTOLOAD;
    }
    elsif ( $self->_accessible( $Attrib, 'record-read' ) ) {
        *{$AUTOLOAD}
            = sub { $_[0]->_to_record( $Attrib, $_[0]->_value($Attrib) ) };
        goto &$AUTOLOAD;
    }
    elsif ( $self->_accessible( $Attrib, 'foreign-collection' ) ) {
        *{$AUTOLOAD} = sub { $_[0]->_collection_value($Attrib) };
        goto &$AUTOLOAD;
    }
    elsif ( $AUTOLOAD =~ /.*::set_(\w+)/o ) {
        $Attrib = $1;

        if ( $self->_accessible( $Attrib, 'write' ) ) {
            *{$AUTOLOAD} = sub {
                return ( $_[0]->_set( field => $Attrib, value => $_[1] ) );
            };
            goto &$AUTOLOAD;
        }
        elsif ( $self->_accessible( $Attrib, 'record-write' ) ) {
            *{$AUTOLOAD} = sub {
                my $self = shift;
                my $val  = shift;

                $val = $val->id
                    if UNIVERSAL::isa( $val, 'Jifty::DBI::Record' );
                return ( $self->_set( field => $Attrib, value => $val ) );
            };
            goto &$AUTOLOAD;
        }
        elsif ( $self->_accessible( $Attrib, 'read' ) ) {
            *{$AUTOLOAD} = sub { return ( 0, 'Immutable field' ) };
            goto &$AUTOLOAD;
        }
        else {
            return ( 0, 'Nonexistant field?' );
        }
    }
    elsif ( $AUTOLOAD =~ /.*::(\w+?)_obj$/o ) {
        $Attrib = $1;
        if ( $self->_accessible( $Attrib, 'object' ) ) {
            *{$AUTOLOAD} = sub {
                return (shift)->_object(
                    field => $Attrib,
                    args  => [@_],
                );
            };
            goto &$AUTOLOAD;
        }
        else {
            return ( 0, 'No object mapping for field' );
        }
    }

    #Previously, I checked for writability here. but I'm not sure that's the
    #right idea. it breaks the ability to do ValidateQueue for a ticket
    #on creation.

    elsif ( $AUTOLOAD =~ /.*::validate_(\w+)/o ) {
        $Attrib = $1;

        *{$AUTOLOAD} = sub { return ( $_[0]->_validate( $Attrib, $_[1] ) ) };
        goto &$AUTOLOAD;
    }

   # TODO: if autoload = 0 or 1 _ then a combination of lowercase and _ chars,
   # turn them into studlycapped phrases

    else {
        my ( $package, $filename, $line );
        ( $package, $filename, $line ) = caller;

        die "$AUTOLOAD Unimplemented in $package. ($filename line $line) \n";
    }

}

=head2 _accessible  KEY MODE

Private method.

Returns undef unless C<KEY> is accessible in C<MODE> otherwise returns C<MODE> value

=cut

sub _accessible {
    my $self = shift;
    my $attr = shift;
    my $mode = lc( shift || '' );

    my $attribute = $self->_class_accessible(@_)->{$attr};
    return unless defined $attribute;
    return $attribute->{$mode};
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

=head2 _class_accessible 

An older way to specify fields attributes in a derived class.
(The current preferred method is by overriding C<schema>; if you do
this and don't override C<_class_accessible>, the module will generate
an appropriate C<_class_accessible> based on your C<schema>.)

Here's an example declaration:

  sub _class_accessible {
    { 
	 Tofu  => { 'read'=>1, 'write'=>1 },
         Maz   => { 'auto'=>1, },
         Roo   => { 'read'=>1, 'auto'=>1, 'public'=>1, },
    };
  }

=cut

sub _class_accessible {
    my $self = shift;

    return $self->_class_accessible_from_schema if $self->can('schema');

    # XXX This is stub code to deal with the old way we used to do _accessible
    # It should never be called by modern code

    my %accessible;
    while ( my $col = shift ) {
        $accessible{$col}->{ lc($_) } = 1 foreach split( /[\/,]/, shift );
    }
    return ( \%accessible );
}

sub _class_accessible_from_schema {
    my $self = shift;

    my $accessible = {};
    foreach my $key ( $self->_primary_keys ) {
        $accessible->{$key} = { 'read' => 1 };
    }

    my $schema = $self->schema;

    for my $field ( keys %$schema ) {
        if ( $schema->{$field}{'TYPE'} ) {
            $accessible->{$field} = { 'read' => 1, 'write' => 1 };
        }
        elsif ( my $refclass = $schema->{$field}{'REFERENCES'} ) {
            if ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Record' ) ) {
                $accessible->{$field}
                    = { 'record-read' => 1, 'record-write' => 1 };
            }
            elsif ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Collection' ) ) {
                $accessible->{$field} = { 'foreign-collection' => 1 };
            }
            else {
                warn "Error: $refclass neither Record nor Collection";
            }
        }
    }

    return $accessible;
}

sub _to_record {
    my $self  = shift;
    my $field = shift;
    my $value = shift;

    return unless defined $value;

    my $schema      = $self->schema;
    my $description = $schema->{$field};

    return unless $description;

    return $value unless $description->{'REFERENCES'};

    my $classname = $description->{'REFERENCES'};

    return unless UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' );

# XXX TODO FIXME perhaps this is not what should be passed to new, but it needs it
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

# sub {{{ readable_attributes

=head2 readable_attributes

Returns an array of the attributes of this class defined as "read" =>
1 in this class' _class_accessible datastructure

=cut

sub readable_attributes {
    my $self     = shift;
    my $ca       = $self->_class_accessible();
    my @readable = grep { $ca->{$_}->{'read'} or $ca->{$_}->{'record-read'} }
        keys %{$ca};
    return (@readable);
}

=head2 writable_attributes

Returns an array of the attributes of this class defined as "write" =>
1 in this class' _class_accessible datastructure

=cut

sub writable_attributes {
    my $self = shift;
    my $ca   = $self->_class_accessible();
    my @writable
        = grep { $ca->{$_}->{'write'} || $ca->{$_}->{'record-write'} }
        keys %{$ca};
    return @writable;
}

=head2 __value

Takes a field name and returns that field's value. Subclasses should
never override __value.

=cut

sub __value {
    my $self  = shift;
    my $field = lc shift;

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

    my $value = $self->{'values'}{$field};

    return $value;
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
        'field'  => undef,
        'value'  => undef,
        'is_sql' => undef,
        @_
    );

    $args{'column'}          = delete $args{'field'};
    $args{'is_sql_function'} = delete $args{'is_sql'};

    my $ret = Class::ReturnValue->new();

    unless ( $args{'column'} ) {
        $ret->as_array( 0, 'No column specified' );
        $ret->as_error(
            errno        => 5,
            do_backtrace => 0,
            message      => "No column specified"
        );
        return ( $ret->return_value );
    }
    my $column = lc $args{'column'};
    if ( !defined( $args{'value'} ) ) {
        $ret->as_array( 0, "No value passed to _set" );
        $ret->as_error(
            errno        => 2,
            do_backtrace => 0,
            message      => "No value passed to _set"
        );
        return ( $ret->return_value );
    }
    elsif ( ( defined $self->__value($column) )
        and ( $args{'value'} eq $self->__value($column) ) )
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
    #

    $args{'value'} = $self->truncate_value( $args{'column'}, $args{'value'} );

    my $method = "validate_" . $args{'column'};
    unless ( $self->$method( $args{'value'} ) ) {
        $ret->as_array( 0, 'Illegal value for ' . $args{'column'} );
        $ret->as_error(
            errno        => 3,
            do_backtrace => 0,
            message      => "Illegal value for " . $args{'column'}
        );
        return ( $ret->return_value );
    }

    $args{'table'}        = $self->table();
    $args{'primary_keys'} = { $self->primary_keys() };

    # The blob handling will destroy $args{'Value'}. But we assign
    # that back to the object at the end. this works around that
    my $unmunged_value = $args{'value'};

    unless ( $self->_handle->knows_blobs ) {

        # Support for databases which don't deal with LOBs automatically
        my $ca  = $self->_class_accessible();
        my $key = $args{'column'};
        if ( $ca->{$key}->{'type'} =~ /^(text|longtext|clob|blob|lob)$/i ) {
            my $bhash
                = $self->_handle->blob_params( $key, $ca->{$key}->{'type'} );
            $bhash->{'value'} = $args{'value'};
            $args{'value'} = $bhash;
        }
    }

    my $val = $self->_handle->update_record_value(%args);
    unless ($val) {
        my $message = $args{'column'}
            . " could not be set to "
            . $args{'value'} . ".";
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
        $self->Load( $self->Id );
    }
    else {
        $self->{'values'}->{"$column"} = $unmunged_value;
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

=head2 truncate_value  KEY VALUE

Truncate a value that's about to be set so that it will fit inside the database'
s idea of how big the column is. 

(Actually, it looks at SearchBuilder's concept of the database, not directly into the db).

=cut

sub truncate_value {
    my $self  = shift;
    my $key   = shift;
    my $value = shift;

    # We don't need to truncate empty things.
    return undef unless ( defined($value) );

    my $metadata = $self->_class_accessible->{$key};

    my $truncate_to;
    if ( $metadata->{'length'} && !$metadata->{'is_numeric'} ) {
        $truncate_to = $metadata->{'length'};
    }
    elsif ( $metadata->{'type'} && $metadata->{'type'} =~ /char\((\d+)\)/ ) {
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

=head2 _object

_object takes a single column name and an array reference.  It creates
new object instance of class specified in _class_accessable structure
and calls load_by_id on recently created object with the current
column value as argument. It uses the array reference as the object
constructor's arguments.  Subclasses can override _object to insert
custom access control or define default contructor arguments.

Note that if you are using a C<schema> with a C<REFERENCES> field, 
this is unnecessary: the method to access the column's value will
automatically turn it into the appropriate object.

=cut

sub _object {
    my $self = shift;
    return $self->__object(@_);
}

sub __object {
    my $self = shift;
    my %args = ( field => '', args => [], @_ );

    my $field = $args{'field'};
    my $class = $self->_accessible( $field, 'object' );

    # Globs magic to be sure that we call 'eval "require $class"' only once
    # because eval is quite slow -- cubic@acronis.ru
    no strict qw( refs );
    my $vglob = ${ $class . '::' }{'VERSION'};
    unless ( $vglob && *$vglob{'SCALAR'} ) {
        eval "require $class";
        die "Couldn't use $class: $@" if ($@);
        unless ( $vglob && *$vglob{'SCALAR'} ) {
            *{ $class . "::VERSION" } = '-1, By DBIx::SerchBuilder';
        }
    }

    my $object = $class->new( @{ $args{'args'} } );
    $object->load_by_id( $self->__value($field) );
    return $object;
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

    # my ($package, $filename, $line) = caller;
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

=head2 loadbycols

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
            my $meta = $self->_class_accessible->{$key};
            $meta->{'type'} ||= '';

            # TODO: type checking should be done in generic way
            if (   $meta->{'is_numeric'}
                || $meta->{'type'}
                =~ /INT|NUMERIC|DECIMAL|REAL|DOUBLE|FLOAT/i )
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

=head2 loadbyid

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

    my ($key);
    foreach $key ( keys %attribs ) {

        if ( $self->_accessible( $key, 'record-write' ) ) {
            $attribs{$key} = $attribs{$key}->id
                if UNIVERSAL::isa( $attribs{$key}, 'Jifty::DBI::Record' );
        }

        #Truncate things that are too long for their datatypes
        $attribs{$key} = $self->truncate_value( $key => $attribs{$key} );

    }
    unless ( $self->_handle->knows_blobs ) {

        # Support for databases which don't deal with LOBs automatically
        my $ca = $self->_class_accessible();
        foreach $key ( keys %attribs ) {
            if ( $ca->{$key}->{'type'} =~ /^(text|longtext|clob|blob|lob)$/i )
            {
                my $bhash = $self->_handle->blob_params( $key,
                    $ca->{$key}->{'type'} );
                $bhash->{'value'} = $attribs{$key};
                $attribs{$key} = $bhash;
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

Returns or sets the name of the current table

=cut

sub table {
    my $self = shift;
    if (@_) {
        $self->{'table'} = shift;
    }
    return ( $self->{'table'} );
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

1;

__END__



=head1 AUTHOR

Jesse Vincent, <jesse@fsck.com> 

Enhancements by Ivan Kohler, <ivan-rt@420.am>

Docs by Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

L<Jifty::DBI>

=cut


