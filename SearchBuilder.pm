# {{{ Version, package, new, etc

package DBIx::SearchBuilder;

use strict;
use vars qw($VERSION);

$VERSION = "0.80";

=head1 NAME

DBIx::SearchBuilder - Perl extension for easy SQL SELECT Statement generation

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
    $self->{'table_links'}      = "";
    $self->{'limit_clause'}     = "";
    $self->{'order'}            = "";
    $self->{'alias_count'}      = 0;
    $self->{'first_row'}        = 0;
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
sub _Handle {
    my $self = shift;
    return ( $self->{'DBIxHandle'} );
}

# }}}

# {{{ sub _DoSearch

sub _DoSearch {
    my $self = shift;
    my ( $QueryString, $Order );

    $QueryString = "SELECT DISTINCT main.* FROM " . $self->_TableAliases . " ";

    $QueryString .= $self->_LeftJoins . " ";

    $QueryString .= $self->_WhereClause . " " . $self->{'table_links'} . " "
      if ( $self->_isLimited > 0 );

    # TODO: GroupBy won't work with postgres.
    # $QueryString .= $self->_GroupByClause. " ";

    $QueryString .= $self->_OrderClause;


    $self->_ApplyLimits(\$QueryString);


    print STDERR "DBIx::SearchBuilder->DoSearch Query:  $QueryString\n"
      if ( $self->DEBUG );

    # {{{ get $self->{'records'} out of the database
    eval { $self->{'records'} = $self->_Handle->dbh->prepare($QueryString); };
    if ($@) {
        warn "$self couldn't prepare '$QueryString' " . $@;
        return (undef);
    }

    if ( !$self->{'records'} ) {
        warn "Error:" . $self->_Handle->dbh->errstr . "\n";
        return (undef);
    }
    eval {
        if ( !$self->{'records'}->execute ) {
            warn "DBIx::SearchBuilder error:"
              . $self->{'records'}->errstr
              . "\n\tQuery String is $QueryString\n";
            return (undef);
        }
    };
    if ($@) {
        warn "$self couldn't execute a search: " . $@;
        return (undef);
    }

    # }}}

    my $counter = 0;

    # {{{ Iterate through all the rows returned and get child objects

    while ( my $row = $self->{'records'}->fetchrow_hashref() ) {

        $self->{'items'}[$counter] = $self->NewItem();
        $self->{'items'}[$counter]->LoadFromHash($row);

        print STDERR "ID is " . $self->{'items'}[$counter]->Id() . "\n"
          if ( $self->DEBUG );

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

    $self->{'must_redo_search'} = 0;

    return ( $self->Count );
}

# }}}

# {{{ sub _DoCount

sub _DoCount {
    my $self = shift;
    my $all  = shift || 0;
    my ( $QueryString, $Order );

    #TODO refactor DoSearch and DoCount such that we only have
    # one place where we build most of the querystring

    $QueryString =
      "SELECT count(DISTINCT main.id) FROM " . $self->_TableAliases . " ";

    $QueryString .= $self->_LeftJoins . " ";

    $QueryString .= $self->_WhereClause . " " . $self->{'table_links'} . " "
      if ( $self->_isLimited > 0 );


    $self->_ApplyLimits(\$QueryString) unless ($all); 


    print STDERR "DBIx::SearchBuilder->DoSearch Query:  $QueryString\n"
      if ( $self->DEBUG );

    # {{{ get count out of the database
    eval { $self->{'records'} = $self->_Handle->dbh->prepare($QueryString); };
    if ($@) {
        warn "$self couldn't prepare '$QueryString' " . $@;
        return (undef);
    }

    if ( !$self->{'records'} ) {
        warn "Error:" . $self->_Handle->dbh->errstr . "\n";
        return (undef);
    }
    eval {
        if ( !$self->{'records'}->execute ) {
            warn "DBIx::SearchBuilder error:"
              . $self->{'records'}->errstr
              . "\n\tQuery String is $QueryString\n";
            return (undef);
        }
    };
    if ($@) {
        warn "$self couldn't execute a search: " . $@;
        return (undef);
    }

    # }}}

    my @row = $self->{'records'}->fetchrow_array();
    $self->{$all?'count_all':'raw_rows'} = $row[0];

    $self->{records}->finish;
    delete $self->{records};

    return ( $row[0] )
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
    
}


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

        if ( $args{'QUOTEVALUE'} && $args{'OPERATOR'} !~ /IS/ ) {
            $args{'VALUE'} = $self->_Handle->dbh->quote( $args{'VALUE'} );
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

    if ( $self->_Handle->CaseSensitive ) {

        unless ( $args{'CASESENSITIVE'} ) {
            $QualifiedField = "lower($QualifiedField)";
            $args{'VALUE'} = lc( $args{'VALUE'} );
        }

    }

    # If the data contains high-bit characters, convert it to hex notation
    $args{'VALUE'} = '0x' . unpack( 'H*', substr( $args{'VALUE'}, 1, -1 ) )
      if $args{'VALUE'} =~ /[^\x00-\x7f]/;

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

# {{{ sub _TableAliases

#Construct a list of tables and aliases suitable for building our SELECT statement
sub _TableAliases {
    my $self = shift;

    # Set up the first alias. for the _main_ table and
    # go through all the other aliases we set up and build the compiled
    # aliases string
    my $compiled_aliases =
      join ( ", ", $self->{'table'} . " main", @{ $self->{'aliases'} } );

    return ($compiled_aliases);
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
FIELD defaults to the primary key of the main table.
ORDER defaults to ASC(ending).  DESC(ending) is also a valid value for OrderBy


=cut

sub OrderBy {
    my $self = shift;
    my %args = ( ALIAS => 'main',
                 FIELD => undef,
                 ORDER => 'ASC',
                 @_ );
    $self->{'order_by_alias'} = $args{'ALIAS'};
    $self->{'order_by_field'} = $args{'FIELD'};
    if ( $args{'ORDER'} =~ /^des/i ) {
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

    my $clause = "";

    #If we don't have an order defined, set the defaults
    unless ( defined $self->{'order_by_field'} ) {
        $self->OrderBy();
    }

    if (     ( $self->{'order_by_field'} )
         and ( $self->{'order_by_alias'} )
         and ( $self->{'order_by_order'} ) ) {

        $clause = "ORDER BY ";
        $clause .= $self->{'order_by_alias'} . "."
          if ( $self->{'order_by_alias'} );
        $clause .= $self->{'order_by_field'};
        $clause .= " " . $self->{'order_by_order'}
          if ( $self->{'order_by_order'} );
    }

    return ($clause);
}

# }}}

# }}}

# {{{ sub _GroupByClause

# Group by main.id.  This will get SB to only return one copy of each row. which is just what we need
# for yanking object collections out of the database.
# and it's less painful than a distinct, since we only need to compare based on main.id.

sub _GroupByClause {
    my $self = shift;
    return ('GROUP BY main.id ');
}

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

# {{{ sub _LeftJoins

# Return the left joins clause

sub _LeftJoins {
    my $self        = shift;
    my $join_clause = '';
    foreach my $join ( keys %{ $self->{'left_joins'} } ) {
        $join_clause .= $self->{'left_joins'}{$join}{'alias_string'} . " ON ";
        $join_clause .=
          join ( ' AND ',
                 values
                                 %{ $self->{'left_joins'}{$join}{'criteria'} }
          );
    }

    return ($join_clause);
}

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
    my %args = ( TYPE   => 'normal',
                 FIELD1 => undef,
                 ALIAS1 => undef,
                 FIELD2 => undef,
                 TABLE2 => undef,
                 ALIAS2 => undef,
                 @_ );

    if ( $args{'TYPE'} =~ /LEFT/i ) {
        my $alias = $self->_GetAlias( $args{'TABLE2'} );

        $self->{'left_joins'}{"$alias"}{'alias_string'} =
          " LEFT JOIN $args{'TABLE2'} as $alias ";

        $self->{'left_joins'}{"$alias"}{'criteria'}{'base_criterion'} =
          " $args{'ALIAS1'}.$args{'FIELD1'} = $alias.$args{'FIELD2'}";

        return ($alias);
    }

    # we need to build the table of links.
    my $clause =
      $args{'ALIAS1'} . "."
      . $args{'FIELD1'} . " = "
      . $args{'ALIAS2'} . "."
      . $args{'FIELD2'};
    $self->{'table_links'} .= " AND $clause ";

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

    unless ( $self->RowsPerPage ) {
        $self->FirstRow(1);
    }
    $self->FirstRow( 1 + ( $self->RowsPerPage * $page ) );
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

1;
__END__

# {{{ POD




=head1 AUTHOR

Copyright (c) 2001 Jesse Vincent, jesse@fsck.com.

All rights reserved.

This library is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.


=head1 SEE ALSO

DBIx::SearchBuilder::Handle, DBIx::SearchBuilder::Record, perl(1).

=cut

# }}}



