#$Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Record.pm,v 1.4 2000/09/15 04:57:53 jesse Exp $
package DBIx::SearchBuilder::Record;

use strict;
use vars qw($VERSION @ISA $AUTOLOAD);


$VERSION = '0.10';

# Preloaded methods go here.

# {{{ sub new 

#instantiate a new record object.

sub new  {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless ($self, $class);
    return $self;
  }

# }}}

# {{{ sub Id and id
sub Id  {
    my $self = shift;
    return ($self->{'values'}->{'id'});
  }

sub id  {
    my $self = shift;
    return ($self->Id);
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

  if ($AUTOLOAD =~ /.*::(\w+)/ && $self->_Accessible($1,'read')) {
    my $Attrib = $1;

    *{$AUTOLOAD} = sub { return ($_[0]->_Value($Attrib))};
    return($self->_Value($Attrib));
  }
    
  elsif ($AUTOLOAD =~ /.*::Set(\w+)/ && $self->_Accessible($1,'write')) {
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
  my $attrib = shift;
  my $mode = shift;
  my %cols = @_;
  
  #return 0 if it's not a valid attribute;
  return undef unless ($cols{"$attrib"});
  
  #  return true if we can $mode $Attrib;
  $cols{$attrib} =~ /$mode/;
}

# }}}

# {{{ sub _Value 
sub _Value  {
  my $self = shift;
  my $field = shift;
  
  $field = lc $field;
  
  return($self->{'values'}->{"$field"});
}

# }}}

# {{{ sub _Set 

sub _Set  {
  my $self = shift;

  my %args = ( Field => undef,
	       Value => undef,
	       IsSQL => undef,
	       @_ );
  my ($error_condition);
  
  if (defined $args{'Field'}) {
      my $field = lc $args{'Field'};
      if ((defined $self->_Value($field))  and
	  ($args{'Value'} eq $self->_Value($field))) {
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


sub Load  {
    my $self = shift;
    my ($package, $filename, $line) = caller;
    return $self->LoadById(@_);
  }

# }}}

# {{{ sub LoadByCol 

sub LoadByCol  {
    my $self = shift;
    my $col = shift;
    my $val = shift;
    
    $val = $self->_Handle->safe_quote($val);
    my $QueryString = "SELECT  * FROM ".$self->Table." WHERE $col = $val";
    return ($self->_LoadFromSQL($QueryString));
  }

# }}}

# {{{ sub LoadById 
sub LoadById  {
    my $self = shift;
    my $id = shift;

    $id = 0 if (!defined($id));
    return ($self->LoadByCol('id',$id));
}
# }}}  

# {{{ sub LoadFromHash
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

    unless ($self->{'values'}{'id'}) {
	warn "something wrong here";
    }

    $self->_DowncaseValuesHash();
    return ($self->{'values'}{'id'});
  }

# }}}

# }}}

# {{{ Routines dealing with creating or deleting rows in the DB

# {{{ sub Create 

sub Create  {
    my $self = shift;
    return ($self->_Handle->Insert($self->Table, @_));
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

=head2 _DownCaseValuesHash

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

=head1 NAME

DBIx::SearchBuilder::Record - Perl extension for subclassing, so you can deal with a Record

=head1 SYNOPSIS

  use DBIx::SearchBuilder::Record;


=head1 DESCRIPTION
DBIx::SearchBuilder::Record is designed to work with DBIx::SearchBuilder.

Docs are forthcoming. If you pester jesse@fsck.com he'll put them together.

Check out Request Tracker at http://www.fsck.com/projects/rt/ for examples of usage.

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1).

=cut

# }}}

