# $Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder.pm,v 1.14 2001/02/28 02:07:05 jesse Exp $

# {{{ Version, package, new, etc

package DBIx::SearchBuilder;

use strict;
use vars qw($VERSION);

$VERSION = "0.22";

=head1 NAME

DBIx::SearchBuilder - Perl extension for easy SQL SELECT Statement generation

=head1 SYNOPSIS

  use DBIx::SearchBuilder;

   ...

=head1 DESCRIPTION



=cut

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

This completely erases all the data in the SearchBuilder object. It's
useful if a subclass is doing funky stuff to keep track of 
a search

=cut

sub CleanSlate {
    my $self = shift;
    $self->RedoSearch();
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
    
    
    $QueryString = "SELECT main.* FROM " . $self->_TableAliases;
    
    $QueryString .= $self->_WhereClause . " ".  $self->{'table_links'}. " " 
      if ($self->_isLimited > 0);
    
    $QueryString .=  $self->_OrderClause . $self->_LimitClause;
    
    print STDERR "DBIx::SearchBuilder->DoSearch Query:  $QueryString\n" ;
     # if ($self->DEBUG);
    
    
    # {{{ get $self->{'records'} out of the database
    eval {
	$self->{'records'} = $self->_Handle->dbh->prepare($QueryString);
    };
    if ($@) {
	warn "$self couldn't prepare '$QueryString' ". $@;
	return(undef);
    }	

    if (!$self->{'records'}) {
	warn "Error:" . $self->_Handle->dbh->errstr . "\n";
	return (undef);
    }
    eval {
	if (!$self->{'records'}->execute) {
	    warn "DBIx::SearchBuilder error:" . $self->{'records'}->errstr . "\n\tQuery String is $QueryString\n";
	    return(undef);
	}
    };
    if ($@) {
	warn "$self couldn't execute a search: ".$@;
	return(undef);
    }
      

    # }}}
    
    
    my $counter = 0;
    
    # {{{ Iterate through all the rows returned and get child objects
    # TODO: this could be made much more efficient
    
    while (my $row = $self->{'records'}->fetchrow_hashref()) {
		
	$self->{'items'}[$counter] = $self->NewItem();
	$self->{'items'}[$counter]->LoadFromHash($row);
	
	print STDERR "ID is ". $self->{'items'}[$counter]->Id()."\n"
	  if ($self->DEBUG);
	

	$counter++;
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

# {{{ sub _LimitClause

# LIMIT clauses are used for restricting ourselves to subsets of the search.

sub _LimitClause {
    my $self = shift;
    my $limit_clause;
    
    if ($self->RowsPerPage) {
	$limit_clause = " LIMIT ";
	if ($self->FirstRow != 0) {
	    $limit_clause .= $self->FirstRow . ", ";
	}
	$limit_clause .= $self->RowsPerPage;
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

# {{{ Methods dealing traversing rows within the found set

# {{{ sub Next 

=head2 Next

Returns the next row from the set as an object of the type defined by sub NewItem.
When the complete set has been iterated through, returns undef and resets the search
such that the following call to Next will start over with the first item retrieved from the database.

=cut

sub Next  {
    my $self = shift;
    my @row;
    
    return(undef) unless ($self->_isLimited);
    
    $self->_DoSearch() if ($self->{'must_redo_search'} != 0);
    
    if ($self->{'itemscount'} < $self->{'rows'}) { #return the next item
	my $item = ($self->{'items'}[$self->{'itemscount'}]);
	$self->{'itemscount'}++;
	return ($item);
    }
    else { #we've gone through the whole list. reset the count.
	$self->GotoFirstItem();
	return(undef);
    }
}

# }}}

# {{{ sub GotoFirstItem

=head2 GotoFirstItem

Starts the recordset counter over from the first item. the next time you call Next,
you'll get the first item returned by the database, as if you'd just started iterating
through the result set.

=cut

sub GotoFirstItem {
  my $self = shift;
  $self->GotoItem(0);
}
# }}}

# {{{ sub GotoItem

=head2 GotoItem

Takes an integer, n.
Sets the record counter to n. the next time you call Next,
you'll get the nth item.

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
    $self->GotoFirstItem();
    return ($self->Next);
}

# }}}

# {{{ ItemsArrayRef 

=head2 ItemsArrayRef

Return a refernece to an array containing all objects found by this search.

=cut

sub ItemsArrayRef {
    my $self = shift;
    
    #If we're not limited, return an empty array
    return [] unless $self->_isLimited;
    
    #Do a search if we need to.
    $self->_DoSearch() if $self->{'must_redo_search'};

    #If we've got any items in the array, return them.
    # Otherwise, return an empty array
    return ($self->{'items'} || []);
}

# }}}

# }}}

# {{{ sub NewItem 

=head2 NewItem

  NewItem must be subclassed. It is used by DBIx::SearchBuilder to create record 
objects for each row returned from the database.

=cut

sub NewItem  {
    my $self = shift;
    
    die "DBIx::SearchBuilder needs to be subclassed. you can't use it directly.\n";
}
# }}}

# {{{ sub RedoSearch

=head2 RedoSearch

Takes no arguments.  Tells DBIx::SearchBuilder that the next time it's asked
for a record, it should requery the database

=cut

sub RedoSearch {
    my $self = shift;
   $self->{'must_redo_search'} = 1;
}

# }}}

# {{{ Routines dealing with Restrictions (where subclauses) 

# {{{ sub UnLimit 

=head2 UnLimit

UnLimit clears all restrictions and causes this object to return all
rows in the primary table.

=cut

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

# VALUE should always be set and will always be quoted. 

# IMO (TobiX) we
# shouldn't use quoted values, we should rather use placeholders and
# pass the arguments when executing the statement.  This will also
# allow us to alter limits and reexecute the search with a low cost by
# keeping the statement handler.

# ENTRYAGGREGATOR can be AND or OR (or anything else valid to aggregate two
clauses in SQL

# OPERATOR is whatever should be putted in between the FIELD and the
# VALUE.

=cut 

sub Limit  {
    my $self = shift;
    my %args = (
		TABLE => $self->{'table'},
		FIELD => undef,
		VALUE => undef,
		ALIAS => undef,
		ENTRYAGGREGATOR => 'or',
		OPERATOR => '=',
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
    
    # We're now limited. people can do searches.
    
    $self->_isLimited(1);
  
    if (defined ($Alias)) {
	return($Alias);
    }
    else {
	return(1);
    }
}

# }}}

# {{{ sub ShowRestrictions 

=head2 ShowRestrictions

Returns the current object's proposed WHERE clause. 

Deprecated.

=cut


sub ShowRestrictions  {
   my $self = shift;
  $self->_CompileGenericRestrictions();
   $self->_CompileSubClauses();
  return($self->{'where_clause'});
  
}

# }}}

# {{{ sub ImportRestrictions 

=head2 ImportRestrictions

Replaces the current object's WHERE clause with the string passed as its argument.

Deprecated

=cut

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
		@_);
    my ($QualifiedField);
    
    #since we're changing the search criteria, we need to redo the search
    $self->RedoSearch();
    

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

# {{{ sub _TableAliases


#Construct a list of tables and aliases suitable for building our SELECT statement
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

# }}}

# {{{ Routines dealing with ordering

# {{{ sub OrderBy

=head2 Orderby PARAMHASH

Orders the returned results by ALIAS.FIELD ORDER. (by default 'main.id ASC')

Takes a paramhash of ALIAS, FIELD and ORDER.  
ALIAS defaults to main
FIELD defaults to the primary key of the main table.
ORDER defaults to ASC(ending).  DESC(ending) is also a valid value for OrderBy


=cut


sub OrderBy {
    my $self = shift;
    my %args = ( ALIAS => 'main',
		 FIELD => $self->{'primary_key'},
		 ORDER => 'ASC',
		 @_
	       );
    $self->{'order_by_alias'} = $args{'ALIAS'};
    $self->{'order_by_field'} = $args{'FIELD'};
    if ($args{'ORDER'} =~ /^des/i) {
	$self->{'order_by_order'} = "DESC";
    }
    else {
	$self->{'order_by_order'} = "ASC";
    }
    
    $self->RedoSearch();
    
}

# }}} 

# {{{ sub _OrderClause

=head2 _OrderClause

returns the ORDER BY clause for the search.

=cut
sub _OrderClause {
    my $self = shift;
    
    #If we don't have an order defined, set the defaults
    unless ((defined $self->{'order_by_alias'}) and 
	    (defined $self->{'order_by_field'}) and
	    (defined $self->{'order_by_order'})) {
	$self->OrderBy();
    }
	    
    return (" ORDER BY " . $self->{'order_by_alias'} . 
	    "." . $self->{'order_by_field'} . 
	    " " . $self->{'order_by_order'});
    
}

# }}}

# }}}

# {{{ routines dealing with table aliases and linking tables

# {{{ sub NewAlias

=head2 NewAlias

Takes the name of a table.
Returns the string of a new Alias for that table, which can be used to Join tables
or to Limit what gets found by a search.

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

Join instructs DBIx::SearchBuilder to join two tables. 
It takes a param hash with keys ALIAS1, FIELD1, ALIAS2 and FIELD2.

ALIAS1 and ALIAS2 are column aliases obtained from $self->NewAlias or a $self->Limit
FIELD1 and FIELD2 are the fields in ALIAS1 and ALIAS2 that should be linked, respectively.

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

# things we'll want to add:
# get aliases
# add restirction clause

# }}}

# {{{ Deal with 'pages' of results'

# {{{ sub NextPage

sub NextPage {
  my $self = shift;
  $self->FirstRow( $self->FirstRow + $self->RowsPerPage );
}

# }}}

# {{{ sub FirstPage
sub FirstPage {
  my $self = shift;
  $self->FirstRow(1);
}
# }}}

# {{{ sub LastPage 

# }}}

# {{{ sub PrevPage

sub PrevPage {
    my $self = shift;
    if (($self->FirstRow - $self->RowsPerPage) > 1) {
	$self->FirstRow( $self->FirstRow - $self->RowsPerPage );
    }
    else {
	$self->FirstRow(1);
    }
}

# }}}


# {{{ sub GotoPage

sub GotoPage {
    my $self = shift;
    my $page = shift;
    
    unless ($self->RowsPerPage) {
	$self->FirstRow(1);
    }	
    $self->FirstRow(1+ ($self->RowsPerPage * $page));
}
# }}}

# {{{ sub RowsPerPage

=head2 RowsPerPage

Limits the number of rows returned by the database.
Optionally, takes an integer which restricts the # of rows returned in a result
Returns the number of rows the database should display.

=cut

sub RowsPerPage {
    my $self = shift;
    $self->{'show_rows'} = shift if (@_);
    
    return ($self->{'show_rows'});
}

# }}}

# {{{ sub FirstRow

=head2 FirstRow

Get or set the first row of the result set the database should return.
Takes an optional single integer argrument. Returns the currently set integer
first row that the database should return.


=cut

# returns the first row
sub FirstRow {
    my $self = shift;
    if (@_) {
	$self->{'first_row'} = shift;
	
	#SQL starts counting at 0
	$self->{'first_row'}--;
	#gotta redo the search if changing pages
	$self->RedoSearch();
    }
    return ($self->{'first_row'});
    }

# }}}

# }}}

# {{{ Public utility methods

# {{{ sub _ItemsCounter

=head2 _ItemsCounter

Returns the current position in the record set.

=cut

sub _ItemsCounter {
    my $self = shift;
    return $self->{'itemscount'};
}

# }}}

# {{{ sub Count 

=head2 Count

Returns the number of records in the set.

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
# }}}

# {{{ sub IsLast

=head2 IsLast

Returns true if the current row is the last record in the set.

=cut

sub IsLast {
    my $self = shift;
    
    if ($self->_ItemsCounter == $self->Count) {
	return (1);
    }	
    else {
	return (undef);
    }
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

1;
__END__

# {{{ POD




=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

DBIx::SearchBuilder::Handle, DBIx::SearchBuilder::Record, perl(1).

=cut

# }}}
