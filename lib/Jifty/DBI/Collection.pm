
package Jifty::DBI::Collection;

use strict;
use vars qw($VERSION);

$VERSION = "1.30_03";

=head1 NAME

Jifty::DBI - Encapsulate SQL queries and rows in simple perl objects

=head1 SYNOPSIS

  use Jifty::DBI;
  
  package My::Things;
  use base qw/Jifty::DBI::Collection/;
  
  sub _init {
      my $self = shift;
      $self->table('Things');
      return $self->SUPER::_init(@_);
  }
  
  sub new_item {
      my $self = shift;
      # MyThing is a subclass of Jifty::DBI::Record
      return(MyThing->new);
  }
  
  package main;

  use Jifty::DBI::Handle;
  my $handle = Jifty::DBI::Handle->new();
  $handle->Connect( Driver => 'SQLite', Database => "my_test_db" );

  my $sb = My::Things->new( handle => $handle );

  $sb->Limit( FIELD => "column_1", VALUE => "matchstring" );

  while ( my $record = $sb->next ) {
      print $record->my_column_name();
  }

=head1 DESCRIPTION

This module provides an object-oriented mechanism for retrieving and updating data in a DBI-accesible database. 

In order to use this module, you should create a subclass of C<Jifty::DBI> and a 
subclass of C<Jifty::DBI::Record> for each table that you wish to access.  (See
the documentation of C<Jifty::DBI::Record> for more information on subclassing it.)

Your C<Jifty::DBI> subclass must override C<new_item>, and probably should override
at least C<_init> also; at the very least, C<_init> should probably call C<_handle> and C<_Table>
to set the database handle (a C<Jifty::DBI::Handle> object) and table name for the class.
You can try to override just about every other method here, as long as you think you know what you
are doing.

=head1 METHOD NAMING
 
Each method has a lower case alias; '_' is used to separate words.
For example, the method C<redo_search> has the alias C<redo_search>.

=head1 METHODS

=cut


=head2 new

Creates a new SearchBuilder object and immediately calls C<_init> with the same parameters
that were passed to C<new>.  If you haven't overridden C<_init> in your subclass, this means
that you should pass in a C<Jifty::DBI::Handle> (or one of its subclasses) like this:

   my $sb = My::Jifty::DBI::Subclass->new( handle => $handle );

However, if your subclass overrides _init you do not need to take a handle argument, as long
as your subclass returns an appropriate handle object from the C<_handle> method.  This is
useful if you want all of your SearchBuilder objects to use a shared global handle and don't want
to have to explicitly pass it in each time, for example.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless( $self, $class );
    $self->_init(@_);
    return ($self);
}



=head2 _init

This method is called by C<new> with whatever arguments were passed to C<new>.  
By default, it takes a C<Jifty::DBI::Handle> object as a C<handle>
argument, although this is not necessary if your subclass overrides C<_handle>.

=cut

sub _init {
    my $self = shift;
    my %args = ( handle => undef,
                 @_ );
    $self->_handle( $args{'handle'} );

    $self->clean_slate();
}



=head2 clean_slate

This completely erases all the data in the SearchBuilder object. It's
useful if a subclass is doing funky stuff to keep track of a search and
wants to reset the SearchBuilder data without losing its own data;
it's probably cleaner to accomplish that in a different way, though.

=cut

sub clean_slate {
    my $self = shift;
    $self->redo_search();
    $self->{'itemscount'}       = 0;
    $self->{'tables'}           = "";
    $self->{'auxillary_tables'} = "";
    $self->{'where_clause'}     = "";
    $self->{'limit_clause'}     = "";
    $self->{'order'}            = "";
    $self->{'alias_count'}      = 0;
    $self->{'first_row'}        = 0;
    $self->{'must_redo_search'} = 1;
    $self->{'show_rows'}        = 0;
    @{ $self->{'aliases'} } = ();

    delete $self->{$_} for qw(
	items
	left_joins
	raw_rows
	count_all
	subclauses
	restrictions
	_open_parens
	_close_parens
    );

    #we have no limit statements. DoSearch won't work.
    $self->_is_limited(0);

}



=head2 _handle  [DBH]

Get or set this object's Jifty::DBI::Handle object.

=cut

sub _handle {
    my $self = shift;
    if (@_) {
        $self->{'DBIxhandle'} = shift;
    }
    return ( $self->{'DBIxhandle'} );
}


    
=head2 _do_search

This internal private method actually executes the search on the database;
it is called automatically the first time that you actually need results
(such as a call to C<Next>).

=cut

sub _do_search {
    my $self = shift;

    my $QueryString = $self->build_select_query();

    # If we're about to redo the search, we need an empty set of items
    delete $self->{'items'};

    my $records = $self->_handle->simple_query($QueryString);
    return 0 unless $records;

    while ( my $row = $records->fetchrow_hashref() ) {
	my $item = $self->new_item();
	$item->load_from_hash($row);
	$self->add_record($item);
    }
    return $self->_record_count if $records->err;

    $self->{'must_redo_search'} = 0;

    return $self->_record_count;
}


=head2 add_record RECORD

Adds a record object to this collection.

=cut

sub add_record {
    my $self = shift;
    my $record = shift;
    push @{$self->{'items'}}, $record;
}

=head2 _record_count

This private internal method returns the number of Record objects saved
as a result of the last query.

=cut

sub _record_count {
    my $self = shift;
    return 0 unless defined $self->{'items'};
    return scalar @{ $self->{'items'} };
}



=head2 _do_count

This internal private method actually executes a counting operation on the database;
it is used by C<Count> and C<count_all>.

=cut


sub _do_count {
    my $self = shift;
    my $all  = shift || 0;

    my $QueryString = $self->build_select_count_query();
    my $records     = $self->_handle->simple_query($QueryString);
    return 0 unless $records;

    my @row = $records->fetchrow_array();
    return 0 if $records->err;

    $self->{ $all ? 'count_all' : 'raw_rows' } = $row[0];

    return ( $row[0] );
}



=head2 _apply_limits STATEMENTREF

This routine takes a reference to a scalar containing an SQL statement. 
It massages the statement to limit the returned rows to only C<< $self->rows_per_page >>
rows, skipping C<< $self->first_row >> rows.  (That is, if rows are numbered
starting from 0, row number C<< $self->first_row >> will be the first row returned.)
Note that it probably makes no sense to set these variables unless you are also
enforcing an ordering on the rows (with C<order_by_cols>, say).

=cut


sub _apply_limits {
    my $self = shift;
    my $statementref = shift;
    $self->_handle->apply_limits($statementref, $self->rows_per_page, $self->first_row);
    $$statementref =~ s/main\.\*/join(', ', @{$self->{columns}})/eg
	    if $self->{columns} and @{$self->{columns}};
}


=head2 _distinct_query STATEMENTREF

This routine takes a reference to a scalar containing an SQL statement. 
It massages the statement to ensure a distinct result set is returned.


=cut

sub _distinct_query {
    my $self = shift;
    my $statementref = shift;
    my $table = shift;

    # XXX - Postgres gets unhappy with distinct and order_by aliases
    if (exists $self->{'order_clause'} && $self->{'order_clause'} =~ /(?<!main)\./) {
        $$statementref = "SELECT main.* FROM $$statementref";
    }
    else {
	$self->_handle->distinct_query($statementref, $table)
    }
}



=head2 _build_joins

Build up all of the joins we need to perform this query.

=cut


sub _build_joins {
    my $self = shift;

        return ( $self->_handle->_build_joins($self) );

}


=head2 _is_joined 

Returns true if this SearchBuilder will be joining multiple tables together.

=cut

sub _is_joined {
    my $self = shift;
    if (keys(%{$self->{'left_joins'}})) {
        return(1);
    } else {
        return(@{$self->{'aliases'}});
    }

}




# LIMIT clauses are used for restricting ourselves to subsets of the search.



sub _limit_clause {
    my $self = shift;
    my $limit_clause;

    if ( $self->rows_per_page ) {
        $limit_clause = " LIMIT ";
        if ( $self->first_row != 0 ) {
            $limit_clause .= $self->first_row . ", ";
        }
        $limit_clause .= $self->rows_per_page;
    }
    else {
        $limit_clause = "";
    }
    return $limit_clause;
}



=head2 _is_limited

If we've limited down this search, return true. Otherwise, return false.

=cut

sub _is_limited {
    my $self = shift;
    if (@_) {
        $self->{'is_limited'} = shift;
    }
    else {
        return ( $self->{'is_limited'} );
    }
}




=head2 build_select_query

Builds a query string for a "SELECT rows from Tables" statement for this SearchBuilder object

=cut

sub build_select_query {
    my $self = shift;

    # The initial SELECT or SELECT DISTINCT is decided later

    my $QueryString = $self->_build_joins . " ";
    $QueryString .= $self->_where_clause . " "
      if ( $self->_is_limited > 0 );

    # DISTINCT query only required for multi-table selects
    if ($self->_is_joined) {
        $self->_distinct_query(\$QueryString, $self->table);
    } else {
        $QueryString = "SELECT main.* FROM $QueryString";
    }

    $QueryString .= ' ' . $self->_group_clause . ' ';

    $QueryString .= ' ' . $self->_order_clause . ' ';

    $self->_apply_limits(\$QueryString);

    return($QueryString)

}



=head2 build_select_count_query

Builds a SELECT statement to find the number of rows this SearchBuilder object would find.

=cut

sub build_select_count_query {
    my $self = shift;

    #TODO refactor DoSearch and do_count such that we only have
    # one place where we build most of the querystring
    my $QueryString = $self->_build_joins . " ";

    $QueryString .= $self->_where_clause . " "
      if ( $self->_is_limited > 0 );



    # DISTINCT query only required for multi-table selects
    if ($self->_is_joined) {
        $QueryString = $self->_handle->distinct_count(\$QueryString);
    } else {
        $QueryString = "SELECT count(main.id) FROM " . $QueryString;
    }

    return ($QueryString);
}




=head2 Next

Returns the next row from the set as an object of the type defined by sub new_item.
When the complete set has been iterated through, returns undef and resets the search
such that the following call to Next will start over with the first item retrieved from the database.

=cut



sub Next {
    my $self = shift;
    my @row;

    return (undef) unless ( $self->_is_limited );

    $self->_do_search() if $self->{'must_redo_search'};

    if ( $self->{'itemscount'} < $self->_record_count ) {    #return the next item
        my $item = ( $self->{'items'}[ $self->{'itemscount'} ] );
        $self->{'itemscount'}++;
        return ($item);
    }
    else {    #we've gone through the whole list. reset the count.
        $self->goto_first_item();
        return (undef);
    }
}



=head2 goto_first_item

Starts the recordset counter over from the first item. The next time you call Next,
you'll get the first item returned by the database, as if you'd just started iterating
through the result set.

=cut


sub goto_first_item {
    my $self = shift;
    $self->goto_item(0);
}




=head2 goto_item

Takes an integer, n.
Sets the record counter to n. the next time you call Next,
you'll get the nth item.

=cut

sub goto_item {
    my $self = shift;
    my $item = shift;
    $self->{'itemscount'} = $item;
}



=head2 First

Returns the first item

=cut

sub first {
    my $self = shift;
    $self->goto_first_item();
    return ( $self->next );
}



=head2 Last

Returns the last item

=cut

sub last {
    my $self = shift;
    $self->goto_item( ( $self->count ) - 1 );
    return ( $self->next );
}



=head2 items_array_ref

Return a refernece to an array containing all objects found by this search.

=cut

sub items_array_ref {
    my $self = shift;

    #If we're not limited, return an empty array
    return [] unless $self->_is_limited;

    #Do a search if we need to.
    $self->_do_search() if $self->{'must_redo_search'};

    #If we've got any items in the array, return them.
    # Otherwise, return an empty array
    return ( $self->{'items'} || [] );
}




=head2 new_item

new_item must be subclassed. It is used by Jifty::DBI to create record 
objects for each row returned from the database.

=cut

sub new_item {
    my $self = shift;

    die
"Jifty::DBI needs to be subclassed. you can't use it directly.\n";
}



=head2 redo_search

Takes no arguments.  Tells Jifty::DBI that the next time it's asked
for a record, it should requery the database

=cut

sub redo_search {
    my $self = shift;
    $self->{'must_redo_search'} = 1;
}




=head2 unlimit

unlimit clears all restrictions and causes this object to return all
rows in the primary table.

=cut

sub unlimit {
    my $self = shift;
    $self->_is_limited(-1);
}



=head2 Limit

Limit takes a hash of parameters with the following keys:

=over 4

=item TABLE 

Can be set to something different than this table if a join is
wanted (that means we can't do recursive joins as for now).  

=item ALIAS

Unless ALIAS is set, the join criterias will be taken from EXT_LINKFIELD
and INT_LINKFIELD and added to the criterias.  If ALIAS is set, new
criterias about the foreign table will be added.

=item FIELD

Column to be checked against.

=item VALUE

Should always be set and will always be quoted. 

=item OPERATOR

OPERATOR is the SQL operator to use for this phrase.  Possible choices include:

=over 4

=item "="

=item "!="

=item "LIKE"

In the case of LIKE, the string is surrounded in % signs.  Yes. this is a bug.

=item "NOT LIKE"

=item "STARTSWITH"

STARTSWITH is like LIKE, except it only appends a % at the end of the string

=item "ENDSWITH"

ENDSWITH is like LIKE, except it prepends a % to the beginning of the string

=back

=item ENTRYAGGREGATOR 

Can be AND or OR (or anything else valid to aggregate two clauses in SQL)

=item CASESENSITIVE

on some databases, such as postgres, setting CASESENSITIVE to 1 will make
this search case sensitive

=back

=cut 

sub Limit {
    my $self = shift;
    my %args = (
        TABLE           => $self->table,
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
    $self->redo_search();

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
            my $tmp = $self->_handle->dbh->quote( $args{'VALUE'} );

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

    $Alias = $self->_generic_restriction(%args);

    warn "No table alias set!"
      unless $Alias;

    # We're now limited. people can do searches.

    $self->_is_limited(1);

    if ( defined($Alias) ) {
        return ($Alias);
    }
    else {
        return (1);
    }
}



=head2 show_restrictions

Returns the current object's proposed WHERE clause. 

Deprecated.

=cut

sub show_restrictions {
    my $self = shift;
    $self->_compile_generic_restrictions();
    $self->_compile_sub_clauses();
    return ( $self->{'where_clause'} );

}



=head2 ImportRestrictions

Replaces the current object's WHERE clause with the string passed as its argument.

Deprecated

=cut

#import a restrictions clause
sub ImportRestrictions {
    my $self = shift;
    $self->{'where_clause'} = shift;
}



sub _generic_restriction {
    my $self = shift;
    my %args = ( TABLE           => $self->table,
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
        if ( $args{'TABLE'} eq $self->table ) {

            # TODO this code assumes no self joins on that table.
            # if someone can name a case where we'd want to do that,
            # I'll change it.

            $args{'ALIAS'} = 'main';
        }

        # {{{ if we're joining, we need to work out the table alias

        else {
            $args{'ALIAS'} = $self->new_alias( $args{'TABLE'} );
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

    print STDERR "$self->_generic_restriction QualifiedField=$QualifiedField\n"
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

    if ( $self->_handle->case_sensitive && defined $args{'VALUE'} && $args{'VALUE'} ne ''  && $args{'VALUE'} ne "''" && ($args{'OPERATOR'} !~/IS/ && $args{'VALUE'} !~ /^null$/i)) {

        unless ( $args{'CASESENSITIVE'} || !$args{'QUOTEVALUE'} ) {
               ( $QualifiedField, $args{'OPERATOR'}, $args{'VALUE'} ) =
                 $self->_handle->_make_clause_case_insensitive( $QualifiedField,
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


sub _open_paren {
    my ( $self, $clause ) = @_;
    $self->{_open_parens}{$clause}++;
}

# Immediate Action
sub _close_paren {
    my ( $self, $clause ) = @_;
    my $restriction = \$self->{'restrictions'}{"$clause"};
    if ( !$$restriction ) {
        $$restriction = " ) ";
    }
    else {
        $$restriction .= " ) ";
    }
}


sub _add_sub_clause {
    my $self      = shift;
    my $clauseid  = shift;
    my $subclause = shift;

    $self->{'subclauses'}{"$clauseid"} = $subclause;

}



sub _where_clause {
    my $self = shift;
    my ( $subclause, $where_clause );

    #Go through all the generic restrictions and build up the "generic_restrictions" subclause
    # That's the only one that SearchBuilder builds itself.
    # Arguably, the abstraction should be better, but I don't really see where to put it.
    $self->_compile_generic_restrictions();

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



#Compile the restrictions to a WHERE Clause

sub _compile_generic_restrictions {
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





=head2 Orderby PARAMHASH

Orders the returned results by ALIAS.FIELD ORDER. (by default 'main.id ASC')

Takes a paramhash of ALIAS, FIELD and ORDER.  
ALIAS defaults to main
FIELD defaults to the primary key of the main table.  Also accepts C<FUNCTION(FIELD)> format
ORDER defaults to ASC(ending).  DESC(ending) is also a valid value for order_by


=cut

sub order_by {
    my $self = shift;
    my %args = ( @_ );

    $self->order_by_cols( \%args );
}

=head2 order_by_cols ARRAY

order_by_cols takes an array of paramhashes of the form passed to order_by.
The result set is ordered by the items in the array.

=cut

sub order_by_cols {
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
    $self->redo_search();
}



=head2 _order_clause

returns the ORDER BY clause for the search.

=cut

sub _order_clause {
    my $self = shift;

    return '' unless $self->{'order_clause'};
    return ($self->{'order_clause'});
}





=head2 group_by  (DEPRECATED)

Alias for the group_by_cols method.

=cut

sub group_by { (shift)->group_by_cols( @_ ) }



=head2 group_by_cols ARRAY_OF_HASHES

Each hash contains the keys ALIAS and FIELD. ALIAS defaults to 'main' if ignored.

=cut

sub group_by_cols {
    my $self = shift;
    my @args = @_;
    my $row;
    my $clause;

    foreach $row ( @args ) {
        my %rowhash = ( ALIAS => 'main',
			FIELD => undef,
			%$row
		      );
        if ($rowhash{'FUNCTION'} ) {
            $clause .= ($clause ? ", " : " ");
            $clause .= $rowhash{'FUNCTION'};

        }
        elsif ( ($rowhash{'ALIAS'}) and
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
    $self->redo_search();
}


=head2 _group_clause

Private function to return the "GROUP BY" clause for this query.

=cut

sub _group_clause {
    my $self = shift;

    return '' unless $self->{'group_clause'};
    return ($self->{'group_clause'});
}





=head2 new_alias

Takes the name of a table.
Returns the string of a new Alias for that table, which can be used to Join tables
or to Limit what gets found by a search.

=cut

sub new_alias {
    my $self  = shift;
    my $table = shift || die "Missing parameter";

    my $alias = $self->_get_alias($table);

    my $subclause = "$table $alias";

    push ( @{ $self->{'aliases'} }, $subclause );

    return $alias;
}



# _get_alias is a private function which takes an tablename and
# returns a new alias for that table without adding something
# to self->{'aliases'}.  This function is used by new_alias
# and the as-yet-unnamed left join code

sub _get_alias {
    my $self  = shift;
    my $table = shift;

    $self->{'alias_count'}++;
    my $alias = $table . "_" . $self->{'alias_count'};

    return ($alias);

}



=head2 Join

Join instructs Jifty::DBI to join two tables.  

The standard form takes a param hash with keys ALIAS1, FIELD1, ALIAS2 and 
FIELD2. ALIAS1 and ALIAS2 are column aliases obtained from $self->new_alias or
a $self->Limit. FIELD1 and FIELD2 are the fields in ALIAS1 and ALIAS2 that 
should be linked, respectively.  For this type of join, this method
has no return value.

Supplying the parameter TYPE => 'left' causes Join to preform a left join.
in this case, it takes ALIAS1, FIELD1, TABLE2 and FIELD2. Because of the way
that left joins work, this method needs a TABLE for the second field
rather than merely an alias.  For this type of join, it will return
the alias generated by the join.

Instead of ALIAS1/FIELD1, it's possible to specify EXPRESSION, to join ALIAS2/TABLE2 on an arbitrary expression.

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

    $self->_handle->Join( SearchBuilder => $self, %args );

}





sub NextPage {
    my $self = shift;
    $self->first_row( $self->first_row + $self->rows_per_page );
}


sub FirstPage {
    my $self = shift;
    $self->first_row(1);
}





sub prev_page {
    my $self = shift;
    if ( ( $self->first_row - $self->rows_per_page ) > 1 ) {
        $self->first_row( $self->first_row - $self->rows_per_page );
    }
    else {
        $self->first_row(1);
    }
}



sub goto_page {
    my $self = shift;
    my $page = shift;

    if ( $self->rows_per_page ) {
    	$self->first_row( 1 + ( $self->rows_per_page * $page ) );
    } else {
        $self->first_row(1);
    }
}



=head2 rows_per_page

Limits the number of rows returned by the database.
Optionally, takes an integer which restricts the # of rows returned in a result
Returns the number of rows the database should display.

=cut

sub rows_per_page {
    my $self = shift;
    $self->{'show_rows'} = shift if (@_);

    return ( $self->{'show_rows'} );
}



=head2 first_row

Get or set the first row of the result set the database should return.
Takes an optional single integer argrument. Returns the currently set integer
first row that the database should return.


=cut

# returns the first row
sub first_row {
    my $self = shift;
    if (@_) {
        $self->{'first_row'} = shift;

        #SQL starts counting at 0
        $self->{'first_row'}--;

        #gotta redo the search if changing pages
        $self->redo_search();
    }
    return ( $self->{'first_row'} );
}





=head2 _items_counter

Returns the current position in the record set.

=cut

sub _items_counter {
    my $self = shift;
    return $self->{'itemscount'};
}



=head2 Count

Returns the number of records in the set.

=cut



sub Count {
    my $self = shift;

    # An unlimited search returns no tickets    
    return 0 unless ($self->_is_limited);


    # If we haven't actually got all objects loaded in memory, we
    # really just want to do a quick count from the database.
    if ( $self->{'must_redo_search'} ) {

        # If we haven't already asked the database for the row count, do that
        $self->_do_count unless ( $self->{'raw_rows'} );

        #Report back the raw # of rows in the database
        return ( $self->{'raw_rows'} );
    }

    # If we have loaded everything from the DB we have an
    # accurate count already.
    else {
        return $self->_record_count;
    }
}



=head2 count_all

Returns the total number of potential records in the set, ignoring any
limit_clause.

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
# 22:28 [Robrt(500@outer.space)] count_all _always_ used the return value of _do_count(), as opposed to Count which would return the cached number of 
#           results returned.
# 22:28 [Robrt(500@outer.space)] IIRC, if you do a search with a Limit, then raw_rows will == Limit.
# 22:31 [msg(Robrt)] ah.
# 22:31 [msg(Robrt)] that actually makes sense
# 22:31 [Robrt(500@outer.space)] You should paste this conversation into the count_all docs.
# 22:31 [msg(Robrt)] perhaps I'll create a new method that _actually_ do that.
# 22:32 [msg(Robrt)] since I'm not convinced it's been doing that correctly


sub count_all {
    my $self = shift;

    # An unlimited search returns no tickets    
    return 0 unless ($self->_is_limited);

    # If we haven't actually got all objects loaded in memory, we
    # really just want to do a quick count from the database.
    if ( $self->{'must_redo_search'} || !$self->{'count_all'}) {
        # If we haven't already asked the database for the row count, do that
        $self->_do_count(1) unless ( $self->{'count_all'} );

        #Report back the raw # of rows in the database
        return ( $self->{'count_all'} );
    }

    # If we have loaded everything from the DB we have an
    # accurate count already.
    else {
        return $self->_record_count;
    }
}




=head2 IsLast

Returns true if the current row is the last record in the set.

=cut

sub IsLast {
    my $self = shift;

    return undef unless $self->count;

    if ( $self->_items_counter == $self->count ) {
        return (1);
    }
    else {
        return (0);
    }
}



sub DEBUG {
    my $self = shift;
    if (@_) {
        $self->{'DEBUG'} = shift;
    }
    return ( $self->{'DEBUG'} );
}







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
            $self->table;
        }
    };

    my $name = ( $args{ALIAS} || 'main' ) . '.' . $args{FIELD};
    if ( my $func = $args{FUNCTION} ) {
        if ( $func =~ /^DISTINCT\s*COUNT$/i ) {
            $name = "COUNT(DISTINCT $name)";
        }
        # If we want to substitute 
        elsif ($func =~ /\?/) {
            $name = join($name,split(/\?/,$func));
        }
        # If we want to call a simple function on the column
        elsif ($func !~ /\(/)  {
            $name = "\U$func\E($name)";
        } else {
            $name = $func;
        }
        
    }

    my $column = "col" . @{ $self->{columns} ||= [] };
    $column = $args{FIELD} if $table eq $self->table and !$args{ALIAS};
    push @{ $self->{columns} }, "$name AS \L$column";
    return $column;
}




=head2 Columns LIST

Specify that we want to load only the columns in LIST

=cut

sub Columns {
    my $self = shift;
    $self->Column( FIELD => $_ ) for @_;
}



=head2 fields TABLE
 
Return a list of fields in TABLE, lowercased.

TODO: Why are they lowercased?

=cut

sub fields {
    my $self  = shift;
    my $table = shift;

    my $dbh = $self->_handle->dbh;

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




=head2 has_field  { TABLE => undef, FIELD => undef }

Returns true if TABLE has field FIELD.
Return false otherwise

=cut

sub has_field {
    my $self = shift;
    my %args = ( FIELD => undef,
                 TABLE => undef,
                 @_);

    my $table = $args{TABLE} or die;
    my $field = $args{FIELD} or die;
    return grep { $_ eq $field } $self->fields($table);
}



=head2 Table [TABLE]

If called with an argument, sets this collection's table.

Always returns this collection's table.

=cut

sub set_table {
    my $self = shift;
    return $self->table(@_);
}

sub table {
    my $self = shift;
    $self->{table} = shift if (@_);
    return $self->{table};
}


if( eval { require capitalization } ) {
	capitalization->unimport( __PACKAGE__ );
}

1;
__END__



=head1 TESTING

In order to test most of the features of C<Jifty::DBI>, you need
to provide C<make test> with a test database.  For each DBI driver that you
would like to test, set the environment variables C<SB_TEST_FOO>, C<SB_TEST_FOO_USER>,
and C<SB_TEST_FOO_PASS> to a database name, database username, and database password,
where "FOO" is the driver name in all uppercase.  You can test as many drivers
as you like.  (The appropriate C<DBD::> module needs to be installed in order for
the test to work.)  Note that the C<SQLite> driver will automatically be tested if C<DBD::Sqlite>
is installed, using a temporary file as the database.  For example:

  SB_TEST_MYSQL=test SB_TEST_MYSQL_USER=root SB_TEST_MYSQL_PASS=foo \
    SB_TEST_PG=test SB_TEST_PG_USER=postgres  make test


=head1 AUTHOR

Copyright (c) 2001-2005 Jesse Vincent, jesse@fsck.com.

All rights reserved.

This library is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.


=head1 SEE ALSO

Jifty::DBI::Handle, Jifty::DBI::Record.

=cut




