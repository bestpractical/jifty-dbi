# $Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder.pm,v 1.3 2000/09/04 16:25:55 jesse Exp $

# {{{ Version, package, new, etc

package DBIx::SearchBuilder;

use strict;
use vars qw($VERSION);

$VERSION = '0.05';

# {{{ sub new 

#instantiate a new object.

sub new  {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = {};
  bless ($self, $class);
  $self->_Init(@_);
  return ($self)
}
# }}}

# }}}
# {{{ Methods dealing with Record objects (navigation)

# {{{ sub Next 

=head2 Next

Returns the next row from the set as an object of the apropriate type.
Returns undef when you're done. Starts over after that

=cut

sub Next  {
  my $self = shift;
  my @row;
  
  if (!$self->_isLimited) {
    return(0);
  }
  if ($self->{'must_redo_search'} != 0) {
    $self->_DoSearch();
  }
  
  if ($self->{'itemscount'} < $self->{'rows'}) {
    #increment the itemcounter.
    $self->{'itemscount'}++;
    #serve out that item
    return ($self->{'items'}[$self->{'itemscount'}]);
  }
  else {
    #we've gone through the whole list.
    #reset the count.
    $self->{'itemscount'} = 0;
    return(undef);
  }
}

# }}}

# {{{ sub GotoFirstItem

=head2 GotoFirstItem

Starts the counter over from 0. the next time you call Next,
you'll get the first item

=cut

sub GotoFirstItem {
  my $self = shift;
  $self->GotoItem(0);
}
# }}}

# {{{ sub GotoItem

=head2 GotoItem

Takes an integer, n.
Sets the counter to n. the next time you call Next,
you'll get the n'th item.

=cut
sub GotoItem {
  my $self = shift;
  my $item = shift;
  $self->{'itemscount'} = $item;
}

# }}}

# {{{ sub First 

=head2 First

Returns the first item

=cut

sub First  {
  my $self = shift;
  #Reset the itemcount
  $self->GotoFirstItem();
  return ($self->Next);
}

# }}}

# {{{ sub NewItem 
sub NewItem  {
  my $self = shift;

  die "DBIx::SearchBuilder needs to be subclassed. you can't use it directly.\n";
}
# }}}

# }}}

# {{{ Routines dealing with Restrictions (where subclauses) and ordering

# {{{ sub UnLimit 
sub UnLimit {
  my $self=shift;
  $self->_isLimited(-1);
}

# }}} 

# {{{ sub Limit 

=head2 Limit

Limit takes a paramhash.

# TABLE can be set to something different than this table if a join is
# wanted (that means we can't do recursive joins as for now).  Unless
# ALIAS is set, the join criterias will be taken from EXT_LINKFIELD
# and INT_LINKFIELD and added to the criterias.  If ALIAS is set, new
# criterias about the foreign table will be added.

# VALUE should always be set and will always be quoted.  Maybe TYPE
# should imply that the value shouldn't be quoted?  IMO (TobiX) we
# shouldn't use quoted values, we should rather use placeholders and
# pass the arguments when executing the statement.  This will also
# allow us to alter limits and reexecute the search with a low cost by
# keeping the statement handler.

# ENTRYAGGREGATOR can be AND or OR (or anything else valid to aggregate two
clauses in SQL

# OPERATOR is whatever should be putted in between the FIELD and the
# VALUE.

# ORDERBY is the SQL ORDERBY

# ORDER can be ASCending or DESCending.

=cut 

sub Limit  {
  my $self = shift;
  my %args = (
	      TABLE => $self->{'table'},
	      FIELD => undef,
	      VALUE => undef,
	      ALIAS => undef,
	      TYPE => undef,  #TODO: figure out why this is here
	      ENTRYAGGREGATOR => 'or',
	      OPERATOR => '=',
	      ORDERBY => undef,
	      ORDER => undef,
	 
	      @_ # get the real argumentlist
	     );
  
  my ($Alias);

  if ($args{'FIELD'}) {
    #If it's a like, we supply the %s around the search term
    if ($args{'OPERATOR'} =~ /LIKE/) {
      $args{'VALUE'} = "%".$args{'VALUE'} ."%";
    }
    $args{'VALUE'} = $self->_Handle->dbh->quote($args{'VALUE'});
  }
  
  $Alias = $self->_GenericRestriction(%args);
  warn "No table alias set!"
      unless $Alias;
  
  # {{{ Set $self->{order} - can be "ASC"ending or "DESC"ending.
  if ($args{'ORDER'}) {
    $self->{'order'} = $args{'ORDER'};
  }
  # }}}

  # {{{ If we're setting an OrderBy, set $self->{'orderby'} here
  if ($args{'ORDERBY'}) {
    # 1. if an alias was passed in, use that. 
    if ($args{'ALIAS'}) {
      $self->{'orderby'} = $args{'ALIAS'}.".".$args{'ORDERBY'};
    }
    # 2. if an alias was generated, use that.
    elsif ($Alias) {
       $self->{'orderby'} = "$Alias.".$args{'ORDERBY'};
     }
    # 3. use the primary
    else {
      $self->{'orderby'} = "main.".$args{'ORDERBY'};
    }
  }

  # }}}

  # We're now limited. people can do searches.

  $self->_isLimited(1);
  
  if (defined ($Alias)) {
    return($Alias);
  }
  else {
    return(0);
  }
}

# }}}

# {{{ sub ShowRestrictions 

#Show Restrictions
sub ShowRestrictions  {
   my $self = shift;
  $self->_CompileGenericRestrictions();
   $self->_CompileSubClauses();
  return($self->{'where_clause'});
  
}

# }}}

# {{{ sub ImportRestrictions 

#import a restrictions clause
sub ImportRestrictions  {
  my $self = shift;
  $self->{'where_clause'} = shift;
}
# }}}

# {{{ sub _GenericRestriction 

sub _GenericRestriction  {
    my $self = shift;
    my %args = (
		TABLE => $self->{'table'},
		FIELD => undef,
		VALUE => undef,	#TODO: $Value should take an array of values and generate 
		                #the proper where clause.
		ALIAS => undef,	     
		ENTRYAGGREGATOR => undef,
		OPERATOR => '=',
		RESTRICTION_TYPE => 'generic', 
		@_);
    my ($QualifiedField);

    #since we're changing the search criteria, we need to redo the search
    $self->{'must_redo_search'}=1;


    # {{{ if there's no alias set, we need to set it

    if (!$args{'ALIAS'}) {
	
	#if the table we're looking at is the same as the main table
	if ($args{'TABLE'} eq $self->{'table'}) {
	    
	    # main is the alias of the "primary table.
	    # TODO this code assumes no self joins on that table. 
	    # if someone can name a case where we'd want to do that, I'll change it.

	    $args{'ALIAS'} = 'main';
	}
	
	# {{{ if we're joining, we need to work out the table alias

	else {
	    $args{'ALIAS'}=$self->NewAlias($args{'TABLE'})
	      or warn;
	}

	# }}}
    }

    # }}}
    
    
    #Set this to the name of the field and the alias.
    $QualifiedField = $args{'ALIAS'}.".".$args{'FIELD'};
    print STDERR "DBIx::SearchBuilder->_GenericRestriction  QualifiedField is $QualifiedField\n" 
      if ($self->DEBUG);
    
    #If we're overwriting this sort of restriction, 
    # TODO: Something seems wrong here ... I kept getting warnings until I putted in all this crap.
    # Shouldn't we have a default setting somewhere? -- TobiX
    if (((exists $args{'ENTRYAGGREGATOR'}) and ($args{'ENTRYAGGREGATOR'}||"") eq 'none') or 
	(!$self->{'restrictions'}{"$QualifiedField"})) {
	$self->{'restrictions'}{"$QualifiedField"} = 
	  "($QualifiedField $args{'OPERATOR'} $args{'VALUE'})";  
	
    }
    else {
	$self->{'restrictions'}{"$QualifiedField"} .= 
	  " $args{'ENTRYAGGREGATOR'} ($QualifiedField $args{'OPERATOR'} $args{'VALUE'})";
	
    }
    
    return ($args{'ALIAS'});
    
}

# }}}

# {{{ sub _AddRestriction
sub _AddSubClause {
    my $self = shift;
    my $clauseid = shift;
    my $subclause = shift;

    $self->{'subclauses'}{"$clauseid"} = $subclause;
    
}
# }}}

# {{{ sub _WhereClause

sub _WhereClause {
 my $self = shift;
 my ($subclause, $where_clause);
  
 #Go through all the generic restrictions and build up the "generic_restrictions" subclause
 # That's the only one that SearchBuilder builds itself.
 # Arguably, the abstraction should be better, but I don't really see where to put it.
 $self->_CompileGenericRestrictions();

 #Go through all restriction types. Build the where clause from the 
 #Various subclauses.
 foreach $subclause (keys %{ $self->{'subclauses'}}) {
     # Now, build up the where clause
   if (defined ($where_clause)) {
     $where_clause .= " AND ";
   }

   warn "$self $subclause doesn't exist"
     if (!defined $self->{'subclauses'}{"$subclause"});
   $where_clause .= $self->{'subclauses'}{"$subclause"};
 }
 
 $where_clause = " WHERE " . $where_clause if ($where_clause ne '');
 return ($where_clause);
}

# }}}

# {{{ sub _CompileGenericRestrictions 

#Compile the restrictions to a WHERE Clause

sub _CompileGenericRestrictions  {
    my $self = shift;
    my ($restriction);
    $self->{'subclauses'}{'generic_restrictions'} = undef;
    
    #Go through all the restrictions of this type. Buld up the generic subclause
    foreach $restriction (keys %{ $self->{'restrictions'}}) {
	if (defined $self->{'subclauses'}{'generic_restrictions'}) {
	    $self->{'subclauses'}{'generic_restrictions'} .= " AND ";
	}
	$self->{'subclauses'}{'generic_restrictions'} .= 
	  "(" . $self->{'restrictions'}{"$restriction"} . ")";
    }
}

# }}}

# {{{ sub _Order 

# This routine returns what the result set should be ordered by
sub _Order {

  my $self = shift;
  my $OrderClause;
  
  if ($self->{'orderby'}) {
    $OrderClause = " ORDER BY ". $self->{'orderby'};
  }
  else {
    $OrderClause = " ORDER BY main.".$self->{'primary_key'}." ";
  }
  return ($OrderClause);
}

# }}}

# {{{ sub _OrderBy 

# This routine returns whether the result set should be sorted Ascending or Descending

sub _OrderBy {
  my $self = shift;
  my $order_by;
  if ($self->{'order'} =~ /^des/i) {
    $order_by = " DESC";
  }
  else {
    $order_by = " ASC";
  }
  return($order_by);
}

# }}}

# }}}

# {{{ routines dealing with table aliases and linking tables

# {{{ sub NewAlias

=head2 NewAlias

Takes the name of a table.
Returns the string of a new Alias for that table

=cut

sub NewAlias {
    my $self = shift;
    my $table = shift || die "Missing parameter";
    
    
    my $alias=$table."_".$self->{'alias_count'};
    
    $self->{'aliases'}[$self->{'alias_count'}]{'alias'} = $alias;
    $self->{'aliases'}[$self->{'alias_count'}]{'table'} = $table;
    
    $self->{'alias_count'}++;
    
    return $alias;
}
# }}}

# {{{ sub Join

=head2 Join

Join takes a param hash with ALIAS1, FIELD1, ALIAS2 and FIELD2

ALIAS1 and ALIAS2 are column aliases obtained from $self->NewAlias or a $self->Limit
FIELD1 and FIELD2 are the fields in ALIAS1 and ALIAS2 that should be linked, respectively


=cut
sub Join {
    my $self = shift;
    my %args = (FIELD1 => undef,
		ALIAS1 => undef,
		FIELD2 => undef,
		ALIAS2 => undef,
		@_);
    # we need to build the table of links.
    my $clause = $args{'ALIAS1'}. ".". $args{'FIELD1'}. " = " . 
		 $args{'ALIAS2'}. ".". $args{'FIELD2'};
    $self->{'table_links'} .= "AND $clause ";
    
}

# }}}
# {{{ sub _TableAliases

sub _TableAliases {
    my $self = shift;
    
    # Set up the first alias. for the _main_ table
    my $compiled_aliases = $self->{'table'}." main";
    
    # Go through all the other aliases we set up and build the compiled
    # aliases string
    
    for my $count (0..($self->{'alias_count'}-1)) {
	$compiled_aliases .= ", ".
	  $self->{'aliases'}[$count]{'table'}. " ".
	    $self->{'aliases'}[$count]{'alias'};
    }

    return ($compiled_aliases);
}

# }}}


# things we'll want to add:
# get aliases
# add restirction clause
# 

# }}}

# {{{ private utility methods

# {{{ sub _Init 

#Initialize the object

sub _Init  {
  my $self = shift;
  my %args = ( Handle => undef,
	       @_
	     );
  $self->{'DBIxHandle'} = $args{'Handle'}; 

  $self->CleanSlate();
}

# }}}

# {{{ sub CleanSlate

=head2 CleanSlate

This erases all the data in the SearchBuilder object. It's
useful if a subclass is doing funky stuff to keep track of 
a search

=cut

sub CleanSlate {
    my $self = shift;
    $self->{'must_redo_search'}=1;
    $self->{'itemscount'}=0;
    $self->{'tables'} = "";
    $self->{'auxillary_tables'} = "";
    $self->{'where_clause'} = "";
    $self->{'table_links'} = "";
    $self->{'limit_clause'} = "";
    $self->{'order'} = "";
    $self->{'alias_count'} = 0;
    $self->{'first_row'} = 0;
    delete $self->{'items'} if ($self->{'items'});
    delete $self->{'subclauses'} if ($self->{'subclauses'});
    delete $self->{'restrictions'} if ($self->{'restrictions'});

    #we have no limit statements. DoSearch won't work.
    $self->_isLimited(0);

}

# {{{ sub _Handle 
sub _Handle  {
  my $self = shift;
  return ($self->{'DBIxHandle'});
}
# }}}

# {{{ sub _DoSearch 

sub _DoSearch  {
    my $self = shift;
    my ($QueryString, $Order);
    

    $QueryString = "SELECT distinct main.* FROM " . $self->_TableAliases;

   
    $QueryString .= $self->_WhereClause . " ".  $self->{'table_links'}. " " 
    if ($self->_isLimited > 0);
    
    $QueryString .=  $self->_Order . $self->_OrderBy . $self->_Limit;
    
      print STDERR "DBIx::SearchBuilder->DoSearch Query:  $QueryString\n" 
	if ($self->DEBUG);
    
    
    
    # {{{ get $self->{'records'} out of the database
  $self->{'records'} = $self->_Handle->dbh->prepare($QueryString);
  
  if (!$self->{'records'}) {
    die "Error:" . $self->_Handle->dbh->errstr . "\n";
  }
  if (!$self->{'records'}->execute) {
    die "DBIx::SearchBuilder error:" . $self->{'records'}->errstr . "\n\tQuery String is $QueryString\n";
  }

  # }}}
    
    
    my $counter = 0;
    
    # {{{ Iterate through all the rows returned and get child objects
    # TODO: this could be made much more efficient
    
    while (my $row = $self->{'records'}->fetchrow_hashref()) {
	
	$counter++;
	
	$self->{'items'}[$counter] = $self->NewItem();
	$self->{'items'}[$counter]->LoadFromHash($row);
	
	print STDERR "ID is ". $self->{'items'}[$counter]->Id()."\n"
	  if ($self->DEBUG);
	
    }
    
    #How many rows did we get out of that?
    $self->{'rows'} = $counter;

    # TODO: It makes sense keeping and reusing the records statement
    # handler.  Anyway, I don't see that we need it anymore with the
    # current design, and the statement handler will not easily be
    # stored persistantly.
    
    $self->{records}->finish;
    delete $self->{records};

    # }}}
    
    $self->{'must_redo_search'}=0;
    
    return($self->Count);
}

# }}}


# {{{ sub _Limit

sub _Limit {
  my $self = shift;
  
  my $limit_clause;
  
  # LIMIT clauses are used for restricting ourselves to subsets of the search.
  if ( $self->Rows) {
    $limit_clause = " LIMIT ";
    if ($self->FirstRow != 0) {
      $limit_clause .= $self->FirstRow . ", ";
    }
    $limit_clause .= $self->Rows;
  }
  else {
    $limit_clause = "";
  }
  return $limit_clause;
}
# }}}

# {{{ sub _isLimited 
sub _isLimited  {
    my $self = shift;
    if (@_) {
	$self->{'is_limited'} = shift;
    }
    else {
	return ($self->{'is_limited'});
    }
}
# }}}

# }}} Private utility methods

# }}}

# {{{ sub Rows

=head2 Rows


Optionally, takes an integer which restricts the # of rows returned in a result
Returns an integer which is the number of rows the database should display.

=cut

sub Rows {
  my $self = shift;
  if (@_) {
    $self->{'show_rows'} = shift;
  }
  return ($self->{'show_rows'});

}
# }}}

# {{{ sub FirstRow

=head2 FirstRow

Set or get what the first row the database should return is.

=cut

# returns the first row
sub FirstRow {
  my $self = shift;
  if (@_) {
    $self->{'first_row'} = shift;

    #SQL starts counting at 0
    $self->{'first_row'}--;
  }
  return ($self->{'first_row'});

}

# }}}

# {{{ Public utility methods

# {{{ Counter, Count and IsLast

=head2 Counter

Returns the current record in the set

=cut

sub Counter {
    return $_[0]->{'itemscount'};
}

# {{{ sub Count 

=head2 Count

Returns the number of records in the set

=cut

sub Count  {
    my $self = shift;
    
    if ($self->{'must_redo_search'}) {
	return ($self->_DoSearch);
    }
    else {
	return($self->{'rows'});
    }
}

=head2 IsLast

returns true if the current Counter
is the last row

=cut

sub IsLast {
    return ($_[0]->{'itemscount'}==$_[0]->{'rows'});
}
# }}}



# {{{ sub DEBUG 
sub DEBUG {
    my $self = shift;
    if (@_) {
	$self->{'DEBUG'} = shift;
      }
    return ($self->{'DEBUG'});
  }

# }}}

# }}}

# }}}

1;
__END__

# {{{ POD

=head1 NAME

DBIx::SearchBuilder - Perl extension for easy SQL SELECT Statement generation

=head1 SYNOPSIS

  use DBIx::SearchBuilder;

  then read the code. (yes, i'm being lame)

=head1 DESCRIPTION

Jesse is lame and hasn't written docs yet

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

DBIx::Handle, DBIx::Record, perl(1).

=cut

# }}}
