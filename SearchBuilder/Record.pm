#$Header/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Record.pm,v 1.21 2001/02/28 21:36:27 jesse Exp $
package DBIx::SearchBuilder::Record;

use strict;
use vars qw($VERSION @ISA $AUTOLOAD);
use Class::ReturnValue;


$VERSION = '$VERSION$';


=head1 NAME

DBIx::SearchBuilder::Record - Perl extension for subclassing, so you can deal with a Record

=head1 SYNOPSIS

  module MyRecord;
  use DBIx::SearchBuilder::Record;
  @ISA = (DBIx::SearchBuilder::Record);
   

  sub _Init {
      my $self = shift;
      my $DBIxHandle = shift; # A DBIx::SearchBuilder::Handle::foo object for your database

      $self->_Handle($DBIxHandle);
      $self->Table("Users");
  }
 

  # Tell Record what the primary keys are
  sub _PrimaryKeys {
	my $self = shift;
	return['id'];
  }

 
  #Preferred and most efficient way to specify fields attributes in a derived
  #class, used by the autoloader to construct Attrib and SetAttrib methods.

  # read: calling $Object->Foo will return the value of this record's Foo column  # write: calling $Object->SetFoo with a single value will set Foo's value in
  #        both the loaded object and the database

  sub _ClassAccessible {
    { 
	 Tofu  => { 'read'=>1, 'write'=>1 },
         Maz   => { 'auto'=>1, },
         Roo   => { 'read'=>1, 'auto'=>1, 'public'=>1, },
    };
  }

  # specifying an _Accessible subroutine in a derived class is depriciated.
  # only '/' and ',' delimiters work, not full regexes
  
  sub _Accessible  {
      my $self = shift;
      my %Cols = (
		  id => 'read', # id is an immutable primary key
		  Username => 'read/write', #read/write.
		  Password => 'write', # password. write only. see sub IsPassword
		  Created => 'read'  # A created date. read-only
		 );
      return $self->SUPER::_Accessible(@_, %Cols);
  }
  
  # A subroutine to check a user's password without ever returning the current password
  #For security purposes, we didn't expose the Password method above
  
  sub IsPassword {
      my $self = shift;
      my $try = shift;
      
      # note two __s in __Value.  Subclasses may muck with _Value, but they should
      # never touch __Value

      if ($try eq $self->__Value('Password')) {
	  return (1);
      }
      else { 
	  return (undef); 
      }
      
  }

 # Override DBIx::SearchBuilder::Create to do some checking on create
 sub Create {
     my $self = shift;
     my %fields = ( UserId => undef,
		    Password => 'default', #Set a default password
		    @_);
     
     #Make sure a userid is specified
     unless ($fields{'UserId'}) {
	 die "No userid specified.";
     }
     
     #Get DBIx::SearchBuilder::Record->Create to do the real work
     return ($self->SUPER::Create( UserId => $fields{'UserId'},
				   Password => $fields{'Password'},
				   Created => time ));
 }

=head1 DESCRIPTION

DBIx::SearchBuilder::Record is designed to work with DBIx::SearchBuilder.


=head2 What is it trying to do. 

DBIx::SB::Record abstracts the agony of writing the common and generally 
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

Enter, DBIx::SearchBuilder::Record. 

With::Record, you can in the simple case, remove all of that code and 
replace it by defining two methods and inheriting some code.  Its pretty 
simple, and incredibly powerful.  For more complex cases, you can, gasp, 
do more complicated things by overriding certain methods.  Lets stick with
the simple case for now. 

The two methods in question are '_Init' and '_ClassAccessible', all they 
really do are define some values and send you on your way.  As you might 
have guessed the '_' suggests that these are private methods, they are. 
They will get called by your record objects constructor.  

  o. '_Init' 
     Defines what table we are talking about, and set a variable to store 
     the database handle. 

  o. '_ClassAccessible
     Defines what operations may be performed on various data selected 
     from the database.  For example you can define fields to be mutable,
     or immutable, there are a few other options but I don't understand 
     what they do at this time. 

And really, thats it.  So lets have some sample code, but first the example
code makes the following assumptions: 

  The database is 'postgres',
  The host is 'reason',
  The login name is 'mhat',
  The database is called 'example', 
  The table is called 'simple', 
  The table looks like so: 

      id     integer     not NULL,   primary_key(id),
      foo    varchar(10),
      bar    varchar(10)


[file:///example/Simple.pm]

000: package Simple; 
001: use DBIx::SearchBuilder::Record;
002: @ISA = (DBIx::SearchBuilder::Record);

This should be pretty obvious, name the package, import ::Record and then 
define ourself as a subclass of ::Record. 


003: 
004: sub _Init {
005:   my $this   = shift; 
006:   my $handle = shift;
007: 
008:   $this->_Handle($handle); 
009:   $this->Table("Simple"); 
010:   
011:   return ($this);
012: }

Here we set our handle and table name, while its not obvious so far, we'll 
see later that $handle (line: 006) gets passed via ::Record::new when a 
new instance is created.  Thats actually an important concept, the DB handle 
is not bound to a single object but rather, its shared across objects. 


013: 
014: sub _ClassAccessible {
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

Its perl, duh. 



Now, on to the code that will actually *do* something with this object. 

[file://examples/ex.pl]
000: use DBIx::SearchBuilder::Handle;
001: use Simple;

Use two packages, the first is where I get the DB handle from, the latter 
is the object I just created. 

002: 
003: my $handle = DBIx::SearchBuilder::Handle->new();
004:    $handle->Connect( 'Driver'   => 'Pg',
005: 		          'Database' => 'test', 
006: 		          'Host'     => 'reason',
007: 		          'User'     => 'mhat',
008: 		          'Password' => '');

Creates a new DBIx::SB::Handle, and then connects to the database using 
that handle.  Pretty straight forward, the password '' is what I use 
when there is no password.  I could probably leave it blank, but I find 
it to be more clear to define it.

009: 
010: my $s = new Simple($handle);
011: 
012: $s->LoadById(1); 

LoadById is one of four 'LoadBy' methods,  as the name suggests it searches
for an row in the database that has id='0'.  ::SearchBuilder has, what I 
think is a bug, in that it current requires there to be an id field. More 
reasonably it also assumes that the id field is unique. LoadById($id) will 
do undefined things if there is >1 row with the same id.  

In addition to LoadById, we also have:

  o. LoadByCol 
     Takes two arguments, a column name and a value.  Again, it will do 
     undefined things if you use non-unique things.  

  o. LoadByCols
     Takes a hash of columns=>values and returns the *first* to match. 
     First is probably lossy across databases vendors. 
 
  o. LoadFromHash
     Populates this record with data from a DBIx::SearchBuilder.  I'm 
     currently assuming that DBIx::SearchBuilder is what we use in 
     cases where we expect > 1 record.  More on this later.


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
024: $s1 = new Simple($handle);
025: $s1->Create('Id'  => 4,
026: 	         'Foo' => 'Foooooo', 
027: 	         'Bar' => 'Barrrrr');

Poof! A new row in the database has been created!  Now lets delete the 
object! 

028:
029: $s1 = undef;
030: $s1 = new Simple($handle);
031: $s1->LoadById(4);
032: $s1->Delete();

And its gone. 



For simple use, thats more or less all there is to it.  The next part of 
this HowTo will discuss using container classes,  overloading, and what 
ever else I think of.



=head1 METHODS

=cut

# Preloaded methods go here.

# {{{ sub new 

#instantiate a new record object.

sub new  {
    my $proto = shift;
   
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless ($self, $class);
    $self->_Init(@_);
    $self->{'_AccessibleCache'} = $self->_ClassAccessible()
      if $self->can('_ClassAccessible');

    $self->{'_PrimaryKeys'} = $self->_PrimaryKeys() 
      if $self->can('_PrimaryKeys');
    return $self;
  }

# }}}

# {{{ sub Id and id

=head2 id

Returns this row's primary key.

=cut


*Id = \&id;

sub id  {
    my $self = shift;
    return ($self->{'values'}->{'id'});
  }

# }}}

=head2 primary_keys
=head2 PrimaryKeys

Matt Knopp owes docs for this function.

=cut



*primary_keys = \&PrimaryKeys;
sub PrimaryKeys { 
    my $self = shift; 
    my %hash = map { $_ => $self->{'values'}->{$_} } @{$self->{'_PrimaryKeys'}};
    return (%hash);
}



# {{{ Routines dealing with getting and setting row data

# {{{ sub DESTROY
sub DESTROY {
    return 1;
}
# }}}

# {{{ sub AUTOLOAD 

sub AUTOLOAD  {
  my $self = shift;
  
  no strict 'refs';

  if ($AUTOLOAD =~ /.*::(\w+)/o &&  $self->_Accessible($1,'read') )  {
    my $Attrib = $1;

    *{$AUTOLOAD} = sub { return ($_[0]->_Value($Attrib))};
    return($self->_Value($Attrib));
  }
    
  elsif ($AUTOLOAD =~ /.*::[sS]et_?(\w+)/o) {

    if ($self->_Accessible($1,'write')) {
      my $Attrib = $1;

      *{$AUTOLOAD} = sub {  
        return ($_[0]->_Set(Field => $Attrib, Value => $_[1]))
      };

      my $Value = shift @_;
      return($self->_Set(Field => $Attrib, Value => $Value));
    } 

    elsif ($self->_Accessible($1, 'read')) {
      *{$AUTOLOAD} = sub {
        return (0, 'Immutable field');
      };
      return(0, 'Immutable field');
    } 
    else { 
      return(0, 'Nonexistant field?');
    }
  }

  #Previously, I checked for writability here. but I'm not sure that's the
  #right idea. it breaks the ability to do ValidateQueue for a ticket
  #on creation.

  elsif ($AUTOLOAD =~ /.*::[vV]alidate_?(\w+)/o ) {
    my $Attrib = $1;

    *{$AUTOLOAD} = sub {  return ($_[0]->_Validate($Attrib, $_[1]))};
    my $Value = shift @_;
    return($self->_Validate($Attrib, $Value));
    }

  
  # TODO: if autoload = 0 or 1 _ then a combination of lowercase and _ chars, 
  # turn them into studlycapped phrases
  
  else {
    my ($package, $filename, $line);
    ($package, $filename, $line) = caller;
    
    die "$AUTOLOAD Unimplemented in $package. ($filename line $line) \n";
  }
  
}

# }}}

# {{{ sub _Accessible 

*_accessible = \&Accessible;
sub _Accessible  {
  my $self = shift;
  my $attr = shift;
  my $mode = lc(shift);
  if ( !defined($self->{'_AccessibleCache'}) && @_ ) {
    my %cols = @_;
    $self->_AccessibleLoad( map { $_ => $cols{$_} }
#      grep !defined($self->{'_AccessibleCache'}->{$_}),
       keys %cols
    );

  }

  #return 0 if it's not a valid attribute;
  return 0 unless defined($self->{'_AccessibleCache'}->{$attr});
  
  #  return true if we can $mode $Attrib;
  local($^W)=0;
  $self->{'_AccessibleCache'}->{$attr}->{$mode} || 0;
}

# }}}

# {{{ sub _AccessibleLoad

=head2 _AccessibleLoad COLUMN => OPERATIONS, ...

=cut


*_accessible_load = \&AccessibleLoad;
sub _AccessibleLoad {
  my $self = shift;
  while ( my $col = shift ) {
    $self->{'_AccessibleCache'}->{$col}->{lc($_)} = 1
      foreach split(/[\/,]/, shift);
  }
}

# }}}

# {{{ sub _ClassAccessible

=head2 _ClassAccessible HASHREF

Preferred and most efficient way to specify fields attributes in a derived
class.

  sub _ClassAccessible {
    { 
	 Tofu  => { 'read'=>1, 'write'=>1 },
         Maz   => { 'auto'=>1, },
         Roo   => { 'read'=>1, 'auto'=>1, 'public'=>1, },
    };
  }

=cut

# }}}

=head2 ReadableAttributes

Returns an array of the attributes of this class defined as "read" => 1 in this class' _ClassAccessible datastructure

=cut

sub ReadableAttributes {
    my $self = shift;
    my $ca = $self->_ClassAccessible();
    my @readable = grep { $ca->{$_}->{read}} keys %{$ca};
    return (@readable);
}


=head2 WritableAttributes

Returns an array of the attributes of this class defined as "write" => 1 in this class' _ClassAccessible datastructure

=cut

sub WritableAttributes {
    my $self = shift;
    my $ca = $self->_ClassAccessible();
    my @writable = grep { $ca->{$_}->{write}} keys %{$ca};
    return (@writable);

}



# {{{ sub __Value {

=head2 __Value

Takes a field name and returns that field's value. Subclasses should never 
overrid __Value.

=cut

*__value = \&__Value;
sub __Value {
 my $self = shift;
 my $field = shift;

  $field = lc $field;

  return($self->{'values'}->{"$field"});


}
# }}}
# {{{ sub _Value 

=head2 _Value

_Value takes a single column name and returns that column's value for this row.
Subclasses can override _Value to insert custom access control.

=cut

*_value = \&_Value;
sub _Value  {
  my $self = shift;
  return ($self->__Value(@_));
}

# }}}

# {{{ sub _Set 

=head2 _Set

_Set takes a single column name and a single unquoted value.
It updates both the in-memory value of this column and the in-database copy.
Subclasses can override _Set to insert custom access control.

=cut

*_set = \&_Set;
sub _Set {
    my $self = shift;
    return ($self->__Set(@_));
}



*__set = \&__Set;
sub __Set {
    my $self = shift;

    my %args = ( 'Field' => undef,
                 'Value' => undef,
                 'IsSQL' => undef,
                 @_ );

    $args{'Column'}        = $args{'Field'};
    $args{'IsSQLFunction'} = $args{'IsSQL'};

    my $ret = Class::ReturnValue->new();

    ## Cleanup the hash.
    delete $args{'Field'};
    delete $args{'IsSQL'};

    unless ( defined( $args{'Column'} ) && $args{'Column'} ) {
        $ret->as_array( 0, 'No column specified' );
        $ret->as_error( errno   => 5,
                        do_backtrace => 0,
                            message => "No column specified" );
        return ($ret->return_value);
    }
    my $column = lc $args{'Column'};
    if (     ( defined $self->__Value($column) )
         and ( $args{'Value'} eq $self->__Value($column) ) ) {
        $ret->as_array( 0, "That is already the current value" );
        $ret->as_error( errno   => 1,
                        do_backtrace => 0,
                            message => "That is already the current value" );
        return ($ret->return_value);
    }
    elsif ( !defined( $args{'Value'} ) ) {
        $ret->as_array( 0, "No value passed to _Set" );
        $ret->as_error( errno   => 2,
                        do_backtrace => 0,
                            message => "No value passed to _Set" );
        return ($ret->return_value);
    }
    else {

        my $method = "Validate" . $args{'Column'};
        unless ( $self->$method( $args{'Value'} ) ) {
            $ret->as_array( 0, 'Illegal value for ' . $args{'Column'} );
            $ret->as_error(errno   => 3,
                        do_backtrace => 0,
                               message => "Illegal value for " . $args{'Column'}
            );
            return ($ret->return_value);
        }

        $args{'Table'}       = $self->Table();
        $args{'PrimaryKeys'} = { $self->PrimaryKeys() };

        my $val = $self->_Handle->UpdateRecordValue(%args);
        unless ($val) {
            $ret->as_array( 0,
                                $args{'Column'}
                                  . " could not be set to "
                                  . $args{'Value'} . "." );
            $ret->as_error( errno   => 4,
                        do_backtrace => 0,
                                message => $args{'Column'}
                                  . " could not be set to "
                                  . $args{'Value'} . "." );
            return ($ret->return_value);
        }
        if ( $args{'IsSQLFunction'} ) {
            $self->Load( $self->Id );
        }
        else {
            $self->{'values'}->{"$column"} = $args{'Value'};
        }
    }
    $ret->as_array( 1, "The new value has been set." );
    return ($ret->return_value);
}

# }}}

# {{{ sub _Validate 

#TODO: Implement _Validate.

*_validate = \&_Validate;
sub _Validate  {
    my $self = shift;
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
   return(1); 
  }	

# }}}	

# }}}
  
# {{{ routines dealing with loading records

# {{{ sub Load 

# load should do a bit of overloading
# if we call it with only one argument, we're trying to load by reference.
# if we call it with a passel of arguments, we're trying to load by value
# The latter is primarily important when we've got a whole set of record that we're
# reading in with a recordset class and want to instantiate objefcts for each record.

=head2 Load

Takes a single argument, $id. Calls LoadByRow to retrieve the row whose primary key
is $id

=cut


*load = \&Load;
sub Load  {
    my $self = shift;
    my ($package, $filename, $line) = caller;
    return $self->LoadById(@_);
  }

# }}}
# {{{ sub LoadByCol 

=head2 LoadByCol

Takes two arguments, a column and a value. The column can be any table column
which contains unique values.  Behavior when using a non-unique value is
undefined

=cut

*load_by_col = \&LoadByCol;

sub LoadByCol  {
    my $self = shift;
    my $col = shift;
    my $val = shift;
    
    return($self->LoadByCols($col => $val));
    
  }

# }}}

# {{{ sub LoadByCols

=head2 LoadByCols

Takes a hash of columns and values. Loads the first record that matches all
keys.

The hash's keys are the columns to look at.

The hash's values are either: scalar values to look for
                        OR has references which contain 'operator' and 'value'




=cut

*load_by_cols = \&LoadByCols;
sub LoadByCols  {
    my $self = shift;
    my %hash  = (@_);
    my (@bind, @phrases);
    foreach my $key (keys %hash) {  
	if (defined $hash{$key} &&  $hash{$key} ne '') {
        my $op;
        my $value;
        if (ref $hash{$key}) {
            $op = $hash{$key}->{operator};
            $value = $hash{$key}->{value};
        } else {
            $op = '=';
            $value = $hash{$key};
        }

		push @phrases, "$key $op ?"; 
		push @bind, $value;
	}
	else {
		push @phrases, "($key IS NULL OR $key = '')";
	}
    }
    
    my $QueryString = "SELECT  * FROM ".$self->Table." WHERE ". 
    join(' AND ', @phrases) ;
    return ($self->_LoadFromSQL($QueryString, @bind));
}


# }}}

# {{{ sub LoadById 

=head2 LoadById

Loads a record by its primary key.
TODO: BUG: Column name is currently hard coded to 'id'

=cut

*load_by_id = \&LoadById;
sub LoadById  {
    my $self = shift;
    my $id = shift;

    $id = 0 if (!defined($id));
    return ($self->LoadByCols('id',$id));
}

# }}}  

*load_by_primary_keys = \&LoadByPrimaryKeys;
sub LoadByPrimaryKeys {
    my ($self, $data) = @_;

    if (ref($data) eq 'HASH') {
       my %cols=();
       foreach (@{$self->{'_PrimaryKeys'}}) {
         $cols{$_}=$data->{$_} if (exists($data->{$_}));
       }
       return ($self->LoadByCols(%cols));
    } 
    else { 
      return (0, "Invalid data");
    }
}



# {{{ sub LoadFromHash

=head2 LoadFromHash

  Takes a hashref, such as created by DBIx::SearchBuilder and populates this record's
loaded values hash.

=cut

*load_from_hash = \&LoadFromHash;

sub LoadFromHash {
  my $self = shift;
  my $hashref = shift;
  
  $self->{'values'} = $hashref;
  #$self->_DowncaseValuesHash();
  return ($self->{'values'}{'id'});
}

# }}}

# {{{ sub _LoadFromSQL 

*load_from_sql = \&LoadFromSQL;


sub _LoadFromSQL {
    my $self        = shift;
    my $QueryString = shift;
    my @bind_values = (@_);

    my $sth = $self->_Handle->SimpleQuery( $QueryString, @bind_values );

    #TODO this only gets the first row. we should check if there are more.


    unless ($sth) {
        return($sth);
    }

    eval { $self->{'values'} = $sth->fetchrow_hashref; };
    if ($@) {
        warn $@;
    }

    unless ( $self->{'values'} ) {

        #warn "something might be wrong here; row not found. SQL: $QueryString";
        return ( 0, "Couldn't find row" );
    }

    #$self->_DowncaseValuesHash();

    ## I guess to be consistant with the old code, make sure the primary  
    ## keys exist.

    eval { $self->PrimaryKeys(); };
    if ($@) {
        return ( 0, "Missing a primary key?: $@" );
    }
    return ( 1, "Found Object" );

}

sub _LoadFromSQLold {
    my $self        = shift;
    my $QueryString = shift;
    my @bind_values = (@_);

    my $sth = $self->_Handle->SimpleQuery( $QueryString, @bind_values );

    #TODO this only gets the first row. we should check if there are more.


    return($sth) unless ($sth) ;

    my $fetched;
    eval { $fetched = $sth->fetchrow_hashref() };
    if ($@) {
        warn $@;
    }

    unless ( $fetched ) {

        #warn "something might be wrong here; row not found. SQL: $QueryString";
        return ( 0, "Couldn't find row" );
    }
    
    $self->{'values'} = $fetched;

    ## I guess to be consistant with the old code, make sure the primary  
    ## keys exist.
      #$self->_DowncaseValuesHash();
    
        ## I guess to be consistant with the old code, make sure the primary  
        ## keys exist.
    
      eval { $self->PrimaryKeys(); };
      if ($@) {
          return ( 0, "Missing a primary key?: $@" );
      }
      return ( 1, "Found Object" );


}

# }}}

# }}}

# {{{ Routines dealing with creating or deleting rows in the DB

# {{{ sub Create 

=head2 Create

Takes an array of key-value pairs and drops any keys that aren't known
as columns for this recordtype

=cut 

*create = \&Create;

sub Create  {
    my $self = shift;
    my %attribs = @_;

    my ($key);
    foreach $key (keys %attribs) {	
	my $method = "Validate$key";
	unless ($self->$method($attribs{$key})) {
		delete	$attribs{$key};
	};
    }
    unless ($self->_Handle->KnowsBLOBs) {
        # Support for databases which don't deal with LOBs automatically
        my $ca = $self->_ClassAccessible();
        foreach $key (keys %attribs) {
            if ($ca->{$key}->{'type'} =~ /^(text|longtext|clob|blob|lob)$/i) {
                my $bhash = $self->_Handle->BLOBParams($key, $ca->{$key}->{'type'});
                $bhash->{'value'} = $attribs{$key};
                $attribs{$key} = $bhash;
            }
        }
    }
    return ($self->_Handle->Insert($self->Table, %attribs));
  }

# }}}

# {{{ sub Delete 

*delete =  \&Delete;


sub Delete  {
    my $self = shift;
    
    #TODO Check to make sure the key's not already listed.
    #TODO Update internal data structure

    ## Constructs the where clause.
    my @bind=();
    my %pkeys=$self->PrimaryKeys();
    my $where  = 'WHERE ';
    foreach my $key (keys %pkeys) {
       $where .= $key . "=?" . " AND ";
       push (@bind, $pkeys{$key});
    }

    $where =~ s/AND\s$//;
    my $QueryString = "DELETE FROM ". $self->Table . ' ' . $where;
    return($self->_Handle->SimpleQuery($QueryString, @bind));
  }

# }}}

# }}}


# {{{ sub Table

=head2 Table

Returns or sets the name of the current Table

=cut

*table = \&Table;

sub Table {
    my $self = shift;
    if (@_) {
          $self->{'table'} = shift;
    }
    return ($self->{'table'});
}

# {{{ Routines dealing with database handles


# {{{ sub _Handle 

=head2 _Handle

Returns or sets the current DBIx::SearchBuilder::Handle object

=cut

*_handle = \&_Handle;
sub _Handle  {
    my $self = shift;
    if (@_) {
      $self->{'DBIxHandle'} = shift;
    }
    return ($self->{'DBIxHandle'});
  }

# }}}


# }}}

# {{{ sub _DowncaseValuesHash

=head2 Private: _DownCaseValuesHash

Takes no parameters and returns no arguments.
This private routine iterates through $self->{'values'} and makes
sure that all keys are lowercase.

=cut

*_downcase_values_hash = \&DowncaseValuesHash;

sub _DowncaseValuesHash {
    my $self = shift;
    my ($key);
    
    foreach $key (keys %{$self->{'values'}}) {
	$self->{'new_values'}->{lc $key} = $self->{'values'}->{$key};
    }
    
    $self->{'values'} = $self->{'new_values'};
    delete $self->{'new_values'};
}

# }}}

1;

__END__

# {{{ POD


=head1 AUTHOR

Jesse Vincent, <jesse@fsck.com> 

Enhancements by Ivan Kohler, <ivan-rt@420.am>

Docs by Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

perl(1).

=cut

# }}}

