#$Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Record.pm,v 1.12 2000/12/24 04:29:15 jesse Exp $
package DBIx::SearchBuilder::Record;

use strict;
use vars qw($VERSION @ISA $AUTOLOAD);


$VERSION = '0.10';


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
      return($self->SUPER::_Init(@_));
  }
  
  # The subroutine _Accessible is used by the autoloader 
  # to construct Attrib and SetAttrib methods.

  # If a hash key, Foo in %Cols has a value matching /read/, then
  # calling $Object->Foo will return the value of this record's Foo column

  # If a hash key, Foo in %Cols has a value matching /write/, then
  # calling $Object->SetFoo with a single value will set Foo's value in both
  # the loaded object and the database.
 
  
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

      if ($try eq $self->__Value('Password') {
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

  if ($AUTOLOAD =~ /.*::(\w+)/ &&  $self->_Accessible($1,'read') )  {
    my $Attrib = $1;

    *{$AUTOLOAD} = sub { return ($_[0]->_Value($Attrib))};
    return($self->_Value($Attrib));
  }
    
  elsif ( ($AUTOLOAD =~ /.*::Set(\w+)/ or
           $AUTOLOAD =~ /.*::set_(\w+)/ ) &&
          $self->_Accessible($1,'write')) {
    my $Attrib = $1;

    *{$AUTOLOAD} = sub {  return ($_[0]->_Set(Field => $Attrib, Value => $_[1]))};
    my $Value = shift @_;
    return($self->_Set(Field => $Attrib, Value => $Value));
    }

  #Previously, I checked for writability here. but I'm not sure that's the
  #right idea. it breaks the ability to do ValidateQueue for a ticket
  #on creation.

  elsif ($AUTOLOAD =~ /.*::Validate(\w+)/ ) {
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

sub _Accessible  {
  my $self = shift;
  my $attr = shift;
  my $mode = shift;
  my %cols = @_;

  #return 0 if it's not a valid attribute;
  return undef unless ($cols{"$attr"});
  
  #  return true if we can $mode $Attrib;
  $cols{$attr} =~ /$mode/i;
}

# }}}


# {{{ sub __Value {

=head2 __Value

Takes a field name and returns that field's value. Subclasses should never 
overrid __Value.

=cut

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

sub _Set {
    my $self = shift;
    return ($self->__Set(@_));
}



sub __Set  {
  my $self = shift;

  my %args = ( Field => undef,
	       Value => undef,
	       IsSQL => undef,
	       @_ );
  my ($error_condition);
  
  if (defined $args{'Field'}) {
      my $field = lc $args{'Field'};
      if ((defined $self->__Value($field))  and
	  ($args{'Value'} eq $self->__Value($field))) {
	  return (0, "That is already the current value");
      } 
      elsif (!defined ($args{'Value'})) {
	  return (0,"No value sent to _Set!\n");
      } 
      else {
	  #TODO $self->_Validate($field, $args{'Value'});
	  $error_condition = $self->_Handle->UpdateTableValue($self->Table, $field,$args{'Value'},$self->id, $args{'IsSQL'});
	  # TODO: Deal with error handling?
	  $self->{'values'}->{"$field"} = $args{'Value'};
      }
  }
  return (1, "The new value has been set.");
}

# }}}

# {{{ sub _Validate 

#TODO: Implement _Validate.
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

=cut

sub LoadByCols  {
    my $self = shift;
    my %hash  = (@_);

    my ($key, @phrases, $val);
    foreach $key (keys %hash) {
	
	$val = $self->_Handle->safe_quote($hash{$key});	
	my $phrase = "$key = $val";
	push (@phrases, $phrase);
    }	
    
    
    
    my $QueryString = "SELECT  * FROM ".$self->Table." WHERE ". 
      join(' AND ', @phrases) ;
    return ($self->_LoadFromSQL($QueryString));
  }

# }}}

# {{{ sub LoadById 

=head2 LoadById

Loads a record by its primary key.
TODO: BUG: Column name is currently hard coded to 'id'

=cut

sub LoadById  {
    my $self = shift;
    my $id = shift;

    $id = 0 if (!defined($id));
    return ($self->LoadByCols('id',$id));
}
# }}}  

# {{{ sub LoadFromHash

=head2 LoadFromHash

  Takes a hashref, such as created by DBIx::SearchBuilder and populates this record's
loaded values hash.

=cut

sub LoadFromHash {
  my $self = shift;
  my $hashref = shift;
  
  $self->{'values'} = $hashref;
  $self->_DowncaseValuesHash();
  return ($self->{'values'}{'id'});
}
# }}}

# {{{ sub _LoadFromSQL 

sub _LoadFromSQL  {
    my $self = shift;
    my $QueryString = shift;
    
    my $sth = $self->_Handle->SimpleQuery($QueryString);
    
    #TODO: COMPATIBILITY PROBLEM with fetchrow_hashref!
    #Some DBMS'es returns uppercase, some returns lowercase,
    #and mysql return mixedcase!

    #TODO this only gets the first row. we should check if there are more.
    $self->{'values'} = $sth->fetchrow_hashref;
    unless ($self->{'values'}) {
#	warn "something might be wrong here; row not found. SQL: $QueryString";
	return undef;
    }

    $self->_DowncaseValuesHash();

    unless ($self->{'values'}{'id'}) {
	warn "No id found for this row";
    }

    return ($self->{'values'}{'id'});
  }

# }}}

# }}}

# {{{ Routines dealing with creating or deleting rows in the DB

# {{{ sub Create 

=head2 Create

Takes an array of key-value pairs and drops any keys that aren't known
as columns for this recordtype

=cut 

sub Create  {
    my $self = shift;

    my %attribs = @_;

    my ($key);
    foreach $key (keys %attribs) {	
	my $validate = 
	"unless (\$self->Validate$key(\$attribs{\$key})) {
		delete	\$attribs{\$key};
	}";
	eval ($validate); #TODO check error conditions	
    }
    return ($self->_Handle->Insert($self->Table, %attribs));
  }

# }}}

# {{{ sub Delete 

sub Delete  {
    my $self = shift;
    
    #TODO Check to make sure the key's not already listed.
    #TODO Update internal data structure
    my $QueryString = "DELETE FROM ".$self->Table . " WHERE id  = ". $self->id();
    return($self->_Handle->SimpleQuery($QueryString));
  }

# }}}

# }}}


# {{{ sub Table

=head2 Table

Returns or sets the name of the current Table

=cut

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

sub _DowncaseValuesHash {
    my $self = shift;
    my ($key);
    
    foreach $key (keys %{$self->{'values'}}) {
	$self->{'new_values'}->{lc $key} = $self->{'values'}->{$key};
    }
    
    $self->{'values'} = $self->{'new_values'};
}

# }}}

1;

__END__

# {{{ POD


=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1).

=cut

# }}}

