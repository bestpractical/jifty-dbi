# {{{ Version, package, new, etc

package DBIx::SearchBuilder;

use strict;
use vars qw($VERSION);

$VERSION = "1.12";

=head1 NAME

DBIx::SearchBuilder - Encapsulate SQL queries and rows in simple perl objects

=head1 SYNOPSIS

  use DBIx::SearchBuilder;

   ...

=head1 DESCRIPTION



=cut

# {{{ sub new

#instantiate a new object.

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless( $self, $class );
    $self->_Init(@_);
    return ($self);
}

# }}}

# {{{ sub _Init

#Initialize the object

sub _Init {
    my $self = shift;
    my %args = ( Handle => undef,
                 @_ );
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
    $self->{'itemscount'}       = 0;
    $self->{'tables'}           = "";
    $self->{'auxillary_tables'} = "";
    $self->{'where_clause'}     = "";
    $self->{'limit_clause'}     = "";
    $self->{'order'}            = "";
    $self->{'alias_count'}      = 0;
    $self->{'first_row'}        = 0;
    $self->{'must_redo_search'} = 1;
    @{ $self->{'aliases'} } = ();

    delete $self->{'items'}        if ( defined $self->{'items'} );
    delete $self->{'left_joins'}   if ( defined $self->{'left_joins'} );
    delete $self->{'raw_rows'}     if ( defined $self->{'raw_rows'} );
    delete $self->{'count_all'}    if ( defined $self->{'count_all'} );
    delete $self->{'subclauses'}   if ( defined $self->{'subclauses'} );
    delete $self->{'restrictions'} if ( defined $self->{'restrictions'} );

    #we have no limit statements. DoSearch won't work.
    $self->_isLimited(0);

}

# }}}

# {{{ sub _Handle

=head2 _Handle  [DBH]

Get or set this object's DBI database handle.

=cut

sub _Handle {
    my $self = shift;
    if (@_) {
        $self->{'DBIxHandle'} = shift;
    }
    return ( $self->{'DBIxHandle'} );
}

# }}}

# {{{ sub _DoSearch

sub _DoSearch {
    my $self = shift;

    my $QueryString = $self->BuildSelectQuery();

    eval {

        # TODO: finer-grained eval and cheking.
       my  $records = $self->_Handle->SimpleQuery($QueryString);
        my $counter;
        $self->{'rows'} = 0;
        while ( my $row = $records->fetchrow_hashref() ) {
            my $item = $self->NewItem();
            $item->LoadFromHash($row);
            $self->AddRecord($item);
        }

        $self->{'must_redo_search'} = 0;
    };

    return ( $self->{'rows'});
}

# }}}

=head2 AddRecord RECORD

    Adds a record object to this collection

=cut

sub AddRecord {
    my $self = shift;
    my $record = shift;
   push @{$self->{'items'}}, $record;
   $self->{'rows'}++; 
}


# {{{ sub _DoCount

sub _DoCount {
    my $self = shift;
    my $all  = shift || 0;

    my $QueryString = $self->BuildSelectCountQuery();
    eval {
        # TODO: finer-grained Eval
        my $records     = $self->_Handle->SimpleQuery($QueryString);

        my @row = $records->fetchrow_array();
        $self->{ $all ? 'count_all' : 'raw_rows' } = $row[0];

        return ( $row[0] );
    };
}

# }}}


=head2 _ApplyLimits STATEMENTREF

This routine takes a reference to a scalar containing an SQL statement. 
It massages the statement to limit the returned rows to $self->RowsPerPage
starting with $self->FirstRow


=cut


sub _ApplyLimits {
    my $self = shift;
    my $statementref = shift;
    $self->_Handle->ApplyLimits($statementref, $self->RowsPerPage, $self->FirstRow);
    $$statementref =~ s/main\.\*/join(', ', @{$self->{columns}})/eg
	if $self->{columns} and @{$self->{columns}};
    if (my $groupby = $self->_GroupClause) {
	$$statementref =~ s/(LIMIT \d+)?$/$groupby $1/;
    }
    
}

# {{{ sub _DistinctQuery

=head2 _DistinctQuery STATEMENTREF

This routine takes a reference to a scalar containing an SQL statement. 
It massages the statement to ensure a distinct result set is returned.


=cut

sub _DistinctQuery {
    my $self = shift;
    my $statementref = shift;
    my $table = shift;

    # XXX - Postgres gets unhappy with distinct and OrderBy aliases
    if (exists $self->{'order_clause'} && $self->{'order_clause'} =~ /(?<!main)\./) {
        $$statementref = "SELECT main.* FROM $$statementref";
    }
    else {
	$self->_Handle->DistinctQuery($statementref, $table)
    }
}

# }}}

# {{{ sub _BuildJoins

=head2 _BuildJoins

Build up all of the joins we need to perform this query

=cut


sub _BuildJoins {
    my $self = shift;

        return ( $self->_Handle->_BuildJoins($self) );

}

# }}}
# {{{ sub _isJoined

=head2 _isJoined 

Returns true if this Searchbuilder requires joins between tables

=cut

sub _isJoined {
    my $self = shift;
    if (keys(%{$self->{'left_joins'}})) {
        return(1);
    } else {
        return(@{$self->{'aliases'}});
    }

}

# }}}


# {{{ sub _LimitClause

# LIMIT clauses are used for restricting ourselves to subsets of the search.



sub _LimitClause {
    my $self = shift;
    my $limit_clause;

    if ( $self->RowsPerPage ) {
        $limit_clause = " LIMIT ";
        if ( $self->FirstRow != 0 ) {
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
sub _isLimited {
    my $self = shift;
    if (@_) {
        $self->{'is_limited'} = shift;
    }
    else {
        return ( $self->{'is_limited'} );
    }
}

# }}}

# }}} Private utility methods

# {{{ BuildSelectQuery

=head2 BuildSelectQuery

Builds a query string for a "SELECT rows from Tables" statement for this SB
object

=cut

sub BuildSelectQuery {
    my $self = shift;

    # The initial SELECT or SELECT DISTINCT is decided later

    my $QueryString = $self->_BuildJoins . " ";
    $QueryString .= $self->_WhereClause . " "
      if ( $self->_isLimited > 0 );

    # DISTINCT query only required for multi-table selects
    if ($self->_isJoined) {
        $self->_DistinctQuery(\$QueryString, $self->{'table'});
    } else {
        $QueryString = "SELECT main.* FROM $QueryString";
    }

    $QueryString .= $self->_OrderClause;

    $self->_ApplyLimits(\$QueryString);

    return($QueryString)

}

# }}}

# {{{ BuildSelectCountQuery

=head2 BuildSelectCountQuery

Builds a SELECT statement to find the number of rows this SB object would find.

=cut

sub BuildSelectCountQuery {
    my $self = shift;

    #TODO refactor DoSearch and DoCount such that we only have
    # one place where we build most of the querystring
    my $QueryString = $self->_BuildJoins . " ";

    $QueryString .= $self->_WhereClause . " "
      if ( $self->_isLimited > 0 );



    # DISTINCT query only required for multi-table selects
    if ($self->_isJoined) {
        $QueryString = $self->_Handle->DistinctCount(\$QueryString);
    } else {
        $QueryString = "SELECT count(main.id) FROM " . $QueryString;
    }

    return ($QueryString);
}

# }}}

# {{{ Methods dealing traversing rows within the found set

# {{{ sub Next

=head2 Next

Returns the next row from the set as an object of the type defined by sub NewItem.
When the complete set has been iterated through, returns undef and resets the search
such that the following call to Next will start over with the first item retrieved from the database.

=cut

sub Next {
    my $self = shift;
    my @row;

    return (undef) unless ( $self->_isLimited );

    $self->_DoSearch() if ( $self->{'must_redo_search'} != 0 );

    if ( $self->{'itemscount'} < $self->{'rows'} ) {    #return the next item
        my $item = ( $self->{'items'}[ $self->{'itemscount'} ] );
        $self->{'itemscount'}++;
        return ($item);
    }
    else {    #we've gone through the whole list. reset the count.
        $self->GotoFirstItem();
        return (undef);
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

sub First {
    my $self = shift;
    $self->GotoFirstItem();
    return ( $self->Next );
}

# }}}

# {{{ sub Last

=head2 Last

Returns the last item

=cut

sub Last {
    my $self = shift;
    $self->GotoItem( ( $self->Count ) - 1 );
    return ( $self->Next );
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
    return ( $self->{'items'} || [] );
}

# }}}

# }}}

# {{{ sub NewItem

=head2 NewItem

  NewItem must be subclassed. It is used by DBIx::SearchBuilder to create record 
objects for each row returned from the database.

=cut

sub NewItem {
    my $self = shift;

    die
"DBIx::SearchBuilder needs to be subclassed. you can't use it directly.\n";
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
    my $self = shift;
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

# ENTRYAGGREGATOR can be AND or OR (or anything else valid to aggregate two
clauses in SQL

OPERATOR is the SQL operator to use for this phrase.  =, !=, LIKE. NOT LIKE
In the case of LIKE, the string ins surrounded in % signs.  Yes. this is a bug.

STARTSWITH is like LIKE, except it only appends a % at the end of the string

ENDSWITH is like LIKE, except it prepends a % to the beginning of the string



on some databases, such as postgres, setting CASESENSITIVE to 1 will make
this search case sensitive

=cut 

sub Limit {
    my $self = shift;
    my %args = (
        TABLE           => $self->{'table'},
        FIELD           => undef,
        VALUE           => undef,
        ALIAS           => undef,
        QUOTEVALUE      => 1,
        ENTRYAGGREGATOR => 'or',
        CASESENSITIVE   => undef,
        OPERATOR        => '=',
        SUBCLAUSE       => undef,
        LEFTJOIN        => undef,
        @_    # get the real argumentlist
    );

    my ($Alias);

    #since we're changing the search criteria, we need to redo the search
    $self->RedoSearch();

    if ( $args{'FIELD'} ) {

        #If it's a like, we supply the %s around the search term
        if ( $args{'OPERATOR'} =~ /LIKE/i ) {
            $args{'VALUE'} = "%" . $args{'VALUE'} . "%";
        }
        elsif ( $args{'OPERATOR'} =~ /STARTSWITH/i ) {
            $args{'VALUE'}    = $args{'VALUE'} . "%";
            $args{'OPERATOR'} = "LIKE";
        }
        elsif ( $args{'OPERATOR'} =~ /ENDSWITH/i ) {
            $args{'VALUE'}    = "%" . $args{'VALUE'};
            $args{'OPERATOR'} = "LIKE";
        }

        #if we're explicitly told not to to quote the value or
        # we're doing an IS or IS NOT (null), don't quote the operator.

        if ( $args{'QUOTEVALUE'} && $args{'OPERATOR'} !~ /IS/i ) {
            my $tmp = $self->_Handle->dbh->quote( $args{'VALUE'} );

            # Accomodate DBI drivers that don't understand UTF8
	    if ($] >= 5.007) {
	        require Encode;
	        if( Encode::is_utf8( $args{'VALUE'} ) ) {
	            Encode::_utf8_on( $tmp );
	        }
            }
	    $args{'VALUE'} = $tmp;
        }
    }

    $Alias = $self->_GenericRestriction(%args);

    warn "No table alias set!"
      unless $Alias;

    # We're now limited. people can do searches.

    $self->_isLimited(1);

    if ( defined($Alias) ) {
        return ($Alias);
    }
    else {
        return (1);
    }
}

# }}}

# {{{ sub ShowRestrictions

=head2 ShowRestrictions

Returns the current object's proposed WHERE clause. 

Deprecated.

=cut

sub ShowRestrictions {
    my $self = shift;
    $self->_CompileGenericRestrictions();
    $self->_CompileSubClauses();
    return ( $self->{'where_clause'} );

}

# }}}

# {{{ sub ImportRestrictions

=head2 ImportRestrictions

Replaces the current object's WHERE clause with the string passed as its argument.

Deprecated

=cut

#import a restrictions clause
sub ImportRestrictions {
    my $self = shift;
    $self->{'where_clause'} = shift;
}

# }}}

# {{{ sub _GenericRestriction

sub _GenericRestriction {
    my $self = shift;
    my %args = ( TABLE           => $self->{'table'},
                 FIELD           => undef,
                 VALUE           => undef,
                 ALIAS           => undef,
                 LEFTJOIN        => undef,
                 ENTRYAGGREGATOR => undef,
                 OPERATOR        => '=',
                 SUBCLAUSE       => undef,
                 CASESENSITIVE   => undef,
                 QUOTEVALUE     => undef,
                 @_ );

    my ( $Clause, $QualifiedField );

    #TODO: $args{'VALUE'} should take an array of values and generate
    # the proper where clause.

    #If we're performing a left join, we really want the alias to be the
    #left join criterion.

    if (    ( defined $args{'LEFTJOIN'} )
         && ( !defined $args{'ALIAS'} ) ) {
        $args{'ALIAS'} = $args{'LEFTJOIN'};
    }

    # {{{ if there's no alias set, we need to set it

    unless ( $args{'ALIAS'} ) {

        #if the table we're looking at is the same as the main table
        if ( $args{'TABLE'} eq $self->{'table'} ) {

            # TODO this code assumes no self joins on that table.
            # if someone can name a case where we'd want to do that,
            # I'll change it.

            $args{'ALIAS'} = 'main';
        }

        # {{{ if we're joining, we need to work out the table alias

        else {
            $args{'ALIAS'} = $self->NewAlias( $args{'TABLE'} );
        }

        # }}}
    }

    # }}}

    # Set this to the name of the field and the alias, unless we've been
    # handed a subclause name

    $QualifiedField = $args{'ALIAS'} . "." . $args{'FIELD'};

    if ( $args{'SUBCLAUSE'} ) {
        $Clause = $args{'SUBCLAUSE'};
    }
    else {
        $Clause = $QualifiedField;
    }

    print STDERR "$self->_GenericRestriction QualifiedField=$QualifiedField\n"
      if ( $self->DEBUG );

    my ($restriction);

    # If we're trying to get a leftjoin restriction, lets set
    # $restriction to point htere. otherwise, lets construct normally

    if ( $args{'LEFTJOIN'} ) {
        $restriction =
          \$self->{'left_joins'}{ $args{'LEFTJOIN'} }{'criteria'}{"$Clause"};
    }
    else {
        $restriction = \$self->{'restrictions'}{"$Clause"};
    }

    # If it's a new value or we're overwriting this sort of restriction,

    if ( $self->_Handle->CaseSensitive && defined $args{'VALUE'} && $args{'VALUE'} ne ''  && $args{'VALUE'} ne "''" && ($args{'OPERATOR'} !~/IS/ && $args{'VALUE'} !~ /^null$/i)) {

        unless ( $args{'CASESENSITIVE'} || !$args{'QUOTEVALUE'} ) {
               ( $QualifiedField, $args{'OPERATOR'}, $args{'VALUE'} ) =
                 $self->_Handle->_MakeClauseCaseInsensitive( $QualifiedField,
                $args{'OPERATOR'}, $args{'VALUE'} );
        }

    }

    my $clause = "($QualifiedField $args{'OPERATOR'} $args{'VALUE'})";

    # Juju because this should come _AFTER_ the EA
    my $prefix = "";
    if ( $self->{_open_parens}{$Clause} ) {
        $prefix = " ( " x $self->{_open_parens}{$Clause};
        delete $self->{_open_parens}{$Clause};
    }

    if ( (     ( exists $args{'ENTRYAGGREGATOR'} )
           and ( $args{'ENTRYAGGREGATOR'} || "" ) eq 'none' )
         or ( !$$restriction )
      ) {

        $$restriction = $prefix . $clause;

    }
    else {
        $$restriction .= $args{'ENTRYAGGREGATOR'} . $prefix . $clause;
    }

    return ( $args{'ALIAS'} );

}

# }}}

# {{{ Parentheses Control
sub _OpenParen {
    my ( $self, $clause ) = @_;
    $self->{_open_parens}{$clause}++;
}

# Immediate Action
sub _CloseParen {
    my ( $self, $clause ) = @_;
    my $restriction = \$self->{'restrictions'}{"$clause"};
    if ( !$$restriction ) {
        $$restriction = " ) ";
    }
    else {
        $$restriction .= " ) ";
    }
}

# }}}

# {{{ sub _AddRestriction
sub _AddSubClause {
    my $self      = shift;
    my $clauseid  = shift;
    my $subclause = shift;

    $self->{'subclauses'}{"$clauseid"} = $subclause;

}

# }}}

# {{{ sub _WhereClause

sub _WhereClause {
    my $self = shift;
    my ( $subclause, $where_clause );

    #Go through all the generic restrictions and build up the "generic_restrictions" subclause
    # That's the only one that SearchBuilder builds itself.
    # Arguably, the abstraction should be better, but I don't really see where to put it.
    $self->_CompileGenericRestrictions();

    #Go through all restriction types. Build the where clause from the
    #Various subclauses.
    foreach $subclause ( keys %{ $self->{'subclauses'} } ) {
        # Now, build up the where clause
        if ( defined($where_clause) ) {
            $where_clause .= " AND ";
        }

        warn "$self $subclause doesn't exist"
          if ( !defined $self->{'subclauses'}{"$subclause"} );
        $where_clause .= $self->{'subclauses'}{"$subclause"};
    }

    $where_clause = " WHERE " . $where_clause if ( $where_clause ne '' );

    return ($where_clause);

}

# }}}

# {{{ sub _CompileGenericRestrictions

#Compile the restrictions to a WHERE Clause

sub _CompileGenericRestrictions {
    my $self = shift;
    my ($restriction);

    delete $self->{'subclauses'}{'generic_restrictions'};

    #Go through all the restrictions of this type. Buld up the generic subclause
    foreach $restriction ( sort keys %{ $self->{'restrictions'} } ) {
        if ( defined $self->{'subclauses'}{'generic_restrictions'} ) {
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
FIELD defaults to the primary key of the main table.  Also accepts C<FUNCTION(FIELD)> format
ORDER defaults to ASC(ending).  DESC(ending) is also a valid value for OrderBy


=cut

sub OrderBy {
    my $self = shift;
    my %args = ( @_ );

    $self->OrderByCols( \%args );
}

=head2 OrderByCols ARRAY

OrderByCols takes an array of paramhashes of the form passed to OrderBy.
The result set is ordered by the items in the array.

=cut

sub OrderByCols {
    my $self = shift;
    my @args = @_;
    my $row;
    my $clause;

    foreach $row ( @args ) {

        my %rowhash = ( ALIAS => 'main',
			FIELD => undef,
			ORDER => 'ASC',
			%$row
		      );
        if ($rowhash{'ORDER'} =~ /^des/i) {
	    $rowhash{'ORDER'} = "DESC";
        }
        else {
	    $rowhash{'ORDER'} = "ASC";
        }

        if ( ($rowhash{'ALIAS'}) and
	     ($rowhash{'FIELD'}) and
             ($rowhash{'ORDER'}) ) {

	    if ($rowhash{'FIELD'} =~ /^(\w+\()(.*\))$/) {
		# handle 'FUNCTION(FIELD)' formatted fields
		$rowhash{'ALIAS'} = $1 . $rowhash{'ALIAS'};
		$rowhash{'FIELD'} = $2;
	    }

            $clause .= ($clause ? ", " : " ");
            $clause .= $rowhash{'ALIAS'} . ".";
            $clause .= $rowhash{'FIELD'} . " ";
            $clause .= $rowhash{'ORDER'};
        }
    }

    if ($clause) {
	$self->{'order_clause'} = "ORDER BY" . $clause;
    }
    else {
	$self->{'order_clause'} = "";
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

    unless ( defined $self->{'order_clause'} ) {
	return "";
    }
    return ($self->{'order_clause'});
}

# }}}

# }}}

# {{{ Routines dealing with grouping

# {{{ GroupBy (OBSOLETE)

=head2 GroupBy

OBSOLUTE. You want GroupByCols

=cut

sub GroupBy {
    my $self = shift;
    $self->GroupByCols( @_);
}
# }}}

# {{{ GroupByCols

=head2 GroupByCols ARRAY_OF_HASHES

Each hash contains the keys ALIAS and FIELD. ALIAS defaults to 'main' if ignored.

=cut

sub GroupByCols {
    my $self = shift;
    my @args = @_;
    my $row;
    my $clause;

    foreach $row ( @args ) {
        my %rowhash = ( ALIAS => 'main',
			FIELD => undef,
			%$row
		      );

        if ( ($rowhash{'ALIAS'}) and
             ($rowhash{'FIELD'}) ) {

            $clause .= ($clause ? ", " : " ");
            $clause .= $rowhash{'ALIAS'} . ".";
            $clause .= $rowhash{'FIELD'};
        }
    }

    if ($clause) {
	$self->{'group_clause'} = "GROUP BY" . $clause;
    }
    else {
	$self->{'group_clause'} = "";
    }
    $self->RedoSearch();
}
# }}} 

# {{{ _GroupClause

=head2 _GroupClause

Private function to return the "GROUP BY" clause for this query.


=cut

sub _GroupClause {
    my $self = shift;

    unless ( defined $self->{'group_clause'} ) {
	    return "";
    }
    return ($self->{'group_clause'});
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
    my $self  = shift;
    my $table = shift || die "Missing parameter";

    my $alias = $self->_GetAlias($table);

    my $subclause = "$table $alias";

    push ( @{ $self->{'aliases'} }, $subclause );

    return $alias;
}

# }}}

# {{{ sub _GetAlias

# _GetAlias is a private function which takes an tablename and
# returns a new alias for that table without adding something
# to self->{'aliases'}.  This function is used by NewAlias
# and the as-yet-unnamed left join code

sub _GetAlias {
    my $self  = shift;
    my $table = shift;

    $self->{'alias_count'}++;
    my $alias = $table . "_" . $self->{'alias_count'};

    return ($alias);

}

# }}}

# {{{ sub Join

=head2 Join

Join instructs DBIx::SearchBuilder to join two tables.  


The standard form takes a param hash with keys ALIAS1, FIELD1, ALIAS2 and 
FIELD2. ALIAS1 and ALIAS2 are column aliases obtained from $self->NewAlias or
a $self->Limit. FIELD1 and FIELD2 are the fields in ALIAS1 and ALIAS2 that 
should be linked, respectively.  For this type of join, this method
has no return value.

Supplying the parameter TYPE => 'left' causes Join to preform a left join.
in this case, it takes ALIAS1, FIELD1, TABLE2 and FIELD2. Because of the way
that left joins work, this method needs a TABLE for the second field
rather than merely an alias.  For this type of join, it will return
the alias generated by the join.

=cut

sub Join {
    my $self = shift;
    my %args = (
        TYPE   => 'normal',
        FIELD1 => undef,
        ALIAS1 => 'main',
        TABLE2 => undef,
        FIELD2 => undef,
        ALIAS2 => undef,
        @_
    );

    $self->_Handle->Join( SearchBuilder => $self, %args );

}

# }}}

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
    if ( ( $self->FirstRow - $self->RowsPerPage ) > 1 ) {
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

    if ( $self->RowsPerPage ) {
    	$self->FirstRow( 1 + ( $self->RowsPerPage * $page ) );
    } else {
        $self->FirstRow(1);
    }
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

    return ( $self->{'show_rows'} );
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
    return ( $self->{'first_row'} );
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



sub Count {
    my $self = shift;

    # An unlimited search returns no tickets    
    return 0 unless ($self->_isLimited);


    # If we haven't actually got all objects loaded in memory, we
    # really just want to do a quick count from the database.
    if ( $self->{'must_redo_search'} ) {

        # If we haven't already asked the database for the row count, do that
        $self->_DoCount unless ( $self->{'raw_rows'} );

        #Report back the raw # of rows in the database
        return ( $self->{'raw_rows'} );
    }

    # If we have loaded everything from the DB we have an
    # accurate count already.
    else {
        return ( $self->{'rows'} );
    }
}

# }}}

# {{{ sub CountAll

=head2 CountAll

Returns the total number of potential records in the set, ignoring any
LimitClause.

=cut

# 22:24 [Robrt(500@outer.space)] It has to do with Caching.
# 22:25 [Robrt(500@outer.space)] The documentation says it ignores the limit.
# 22:25 [Robrt(500@outer.space)] But I don't believe thats true.
# 22:26 [msg(Robrt)] yeah. I
# 22:26 [msg(Robrt)] yeah. I'm not convinced it does anything useful right now
# 22:26 [msg(Robrt)] especially since until a week ago, it was setting one variable and returning another
# 22:27 [Robrt(500@outer.space)] I remember.
# 22:27 [Robrt(500@outer.space)] It had to do with which Cached value was returned.
# 22:27 [msg(Robrt)] (given that every time we try to explain it, we get it Wrong)
# 22:27 [Robrt(500@outer.space)] Because Count can return a different number than actual NumberOfResults
# 22:28 [msg(Robrt)] in what case?
# 22:28 [Robrt(500@outer.space)] CountAll _always_ used the return value of _DoCount(), as opposed to Count which would return the cached number of 
#           results returned.
# 22:28 [Robrt(500@outer.space)] IIRC, if you do a search with a Limit, then raw_rows will == Limit.
# 22:31 [msg(Robrt)] ah.
# 22:31 [msg(Robrt)] that actually makes sense
# 22:31 [Robrt(500@outer.space)] You should paste this conversation into the CountAll docs.
# 22:31 [msg(Robrt)] perhaps I'll create a new method that _actually_ do that.
# 22:32 [msg(Robrt)] since I'm not convinced it's been doing that correctly



sub CountAll {
    my $self = shift;

    # An unlimited search returns no tickets    
    return 0 unless ($self->_isLimited);

    # If we haven't actually got all objects loaded in memory, we
    # really just want to do a quick count from the database.
    if ( $self->{'must_redo_search'} || !$self->{'count_all'}) {
        # If we haven't already asked the database for the row count, do that
        $self->_DoCount(1) unless ( $self->{'count_all'} );

        #Report back the raw # of rows in the database
        return ( $self->{'count_all'} );
    }

    # If we have loaded everything from the DB we have an
    # accurate count already.
    else {
        return ( $self->{'rows'} );
    }
}

# }}}


# {{{ sub IsLast

=head2 IsLast

Returns true if the current row is the last record in the set.

=cut

sub IsLast {
    my $self = shift;

    if ( $self->_ItemsCounter == $self->Count ) {
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
    return ( $self->{'DEBUG'} );
}

# }}}




# }}}

# {{{ Column

=head2 Column { FIELD => undef } 

Specify that we want to load the column  FIELD. 

Other parameters are TABLE ALIAS AND FUNCTION.

Autrijus and Ruslan owe docs.

=cut

sub Column {
    my $self = shift;
    my %args = ( TABLE => undef,
               ALIAS => undef,
               FIELD => undef,
               FUNCTION => undef,
               @_);

    my $table = $args{TABLE} || do {
        if ( my $alias = $args{ALIAS} ) {
            $alias =~ s/_\d+$//;
            $alias;
        }
        else {
            $self->{table};
        }
    };

    my $name = ( $args{ALIAS} || 'main' ) . '.' . $args{FIELD};
    if ( my $func = $args{FUNCTION} ) {
        if ( $func =~ /^DISTINCT\s*COUNT$/i ) {
            $name = "COUNT(DISTINCT $name)";
        }
        else {
            $name = "\U$func\E($name)";
        }
    }

    my $column = "col" . @{ $self->{columns} ||= [] };
    $column = $args{FIELD} if $table eq $self->{table} and !$args{ALIAS};
    push @{ $self->{columns} }, "$name AS \L$column";
    return $column;
}

# }}}

# {{{ Columns 


=head2 Columns LIST

Specify that we want to load only the columns in LIST

=cut

sub Columns {
    my $self = shift;
    $self->Column( FIELD => $_ ) for @_;
}

# }}}

# {{{ Fields

=head2 Fields TABLE
 
Return a list of fields in TABLE, lowercased.

TODO: Why are they lowercased?

=cut

sub Fields {
    my $self  = shift;
    my $table = shift;

    my $dbh = $self->_Handle->dbh;

    # TODO: memoize this

    return map lc( $_->[0] ), @{
        eval {
            $dbh->column_info( '', '', $table, '' )->fetchall_arrayref( [3] );
          }
          || $dbh->selectall_arrayref("DESCRIBE $table;")
          || $dbh->selectall_arrayref("DESCRIBE \u$table;")
          || []
      };
}

# }}}


# {{{ HasField

=head2 HasField  { TABLE => undef, FIELD => undef }

Returns true if TABLE has field FIELD.
Return false otherwise

=cut

sub HasField {
    my $self = shift;
    my %args = ( FIELD => undef,
                 TABLE => undef,
                 @_);

    my $table = $args{TABLE} or die;
    my $field = $args{FIELD} or die;
    return grep { $_ eq $field } $self->Fields($table);
}

# }}}

# {{{ SetTable

=head2 Table [TABLE]

If called with an arguemnt, sets this collection's table.

Always returns this collection's table.

=cut

sub SetTable {
    my $self = shift;
    return $self->Table(@_);
}

sub Table {
    my $self = shift;
    $self->{table} = shift if (@_);
    return $self->{table};
}


# }}}


1;
__END__

# {{{ POD




=head1 AUTHOR

Copyright (c) 2001-2004 Jesse Vincent, jesse@fsck.com.

All rights reserved.

This library is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.


=head1 SEE ALSO

DBIx::SearchBuilder::Handle, DBIx::SearchBuilder::Record, perl(1).

=cut

# }}}



