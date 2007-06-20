package Jifty::DBI::Collection;

use warnings;
use strict;

=head1 NAME

Jifty::DBI::Collection - Encapsulate SQL queries and rows in simple
perl objects

=head1 SYNOPSIS

  use Jifty::DBI::Collection;
  
  package My::ThingCollection;
  use base qw/Jifty::DBI::Collection/;

  package My::Thing;
  use Jifty::DBI::Schema;
  use Jifty::DBI::Record schema {
    column column_1 => type is 'text';
  };
  
  package main;

  use Jifty::DBI::Handle;
  my $handle = Jifty::DBI::Handle->new();
  $handle->connect( driver => 'SQLite', database => "my_test_db" );

  my $sb = My::ThingCollection->new( handle => $handle );

  $sb->limit( column => "column_1", value => "matchstring" );

  while ( my $record = $sb->next ) {
      print $record->id;
  }

=head1 DESCRIPTION

This module provides an object-oriented mechanism for retrieving and
updating data in a DBI-accessible database.

In order to use this module, you should create a subclass of
L<Jifty::DBI::Collection> and a subclass of L<Jifty::DBI::Record> for
each table that you wish to access.  (See the documentation of
L<Jifty::DBI::Record> for more information on subclassing it.)

Your L<Jifty::DBI::Collection> subclass must override L</new_item>,
and probably should override at least L</_init> also; at the very
least, L</_init> should probably call L</_handle> and L</_table> to
set the database handle (a L<Jifty::DBI::Handle> object) and table
name for the class -- see the L</SYNOPSIS> for an example.


=cut

use vars qw($VERSION);

use Data::Page;
use Clone;
use Carp qw/croak/;
use base qw/Class::Accessor::Fast/;
__PACKAGE__->mk_accessors(qw/pager preload_columns preload_related/);

=head1 METHODS

=head2 new

Creates a new L<Jifty::DBI::Collection> object and immediately calls
L</_init> with the same parameters that were passed to L</new>.  If
you haven't overridden L<_init> in your subclass, this means that you
should pass in a L<Jifty::DBI::Handle> (or one of its subclasses) like
this:

   my $sb = My::Jifty::DBI::Subclass->new( handle => $handle );

However, if your subclass overrides L</_init> you do not need to take
a handle argument, as long as your subclass takes care of calling the
L</_handle> method somehow.  This is useful if you want all of your
L<Jifty::DBI> objects to use a shared global handle and don't want to
have to explicitly pass it in each time, for example.

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};
    bless( $self, $class );
    $self->record_class( $proto->record_class ) if ref $proto;
    $self->_init(@_);
    return ($self);
}

=head2 _init

This method is called by L<new> with whatever arguments were passed to
L</new>.  By default, it takes a C<Jifty::DBI::Handle> object as a
C<handle> argument and calls L</_handle> with that.

=cut

sub _init {
    my $self = shift;
    my %args = (
        handle => undef,
        @_
    );
    $self->_handle( $args{'handle'} ) if ( $args{'handle'} );
    $self->table( $self->new_item->table() );
    $self->clean_slate(%args);
}

sub _init_pager {
    my $self = shift;
    $self->pager( Data::Page->new );

    $self->pager->total_entries(0);
    $self->pager->entries_per_page(10);
    $self->pager->current_page(1);
}

=head2 clean_slate

This completely erases all the data in the object. It's useful if a
subclass is doing funky stuff to keep track of a search and wants to
reset the object's data without losing its own data; it's probably
cleaner to accomplish that in a different way, though.

=cut

sub clean_slate {
    my $self = shift;
    my %args = (@_);
    $self->redo_search();
    $self->_init_pager();
    $self->{'itemscount'}       = 0;
    $self->{'tables'}           = "";
    $self->{'auxillary_tables'} = "";
    $self->{'where_clause'}     = "";
    $self->{'limit_clause'}     = "";
    $self->{'order'}            = "";
    $self->{'alias_count'}      = 0;
    $self->{'first_row'}        = 0;
    $self->{'show_rows'}        = 0;
    @{ $self->{'aliases'} } = ();

    delete $self->{$_} for qw(
        items
        leftjoins
        raw_rows
        count_all
        subclauses
        restrictions
        _open_parens
        criteria_count
    );

    $self->implicit_clauses(%args);
    $self->_is_limited(0);
}

=head2 implicit_clauses

Called by L</clean_slate> to set up any implicit clauses that the
collection B<always> has.  Defaults to doing nothing. Is passed the
paramhash passed into L</new>.

=cut

sub implicit_clauses { }

=head2 _handle [DBH]

Get or set this object's L<Jifty::DBI::Handle> object.

=cut

sub _handle {
    my $self = shift;
    if (@_) {
        $self->{'DBIxhandle'} = shift;
    }
    return ( $self->{'DBIxhandle'} );
}

=head2 _do_search

This internal private method actually executes the search on the
database; it is called automatically the first time that you actually
need results (such as a call to L</next>).

=cut

sub _do_search {
    my $self = shift;

    my $query_string = $self->build_select_query();

    # If we're about to redo the search, we need an empty set of items
    delete $self->{'items'};

    my $records = $self->_handle->simple_query($query_string);
    return 0 unless $records;
    my @names = @{ $records->{NAME_lc} };
    my $data = {};
    my $column_map = {};
    foreach my $column (@names) {
        if ($column =~ /^((\w+)_?(?:\d*))_(.*?)$/) {
            $column_map->{$1}->{$2} =$column;
        }
    }
    my @tables = keys %$column_map;


    my @order;
    while ( my $base_row = $records->fetchrow_hashref() ) {
        my $main_pkey = $base_row->{$names[0]};
        push @order, $main_pkey unless ( $order[0] && $order[-1] eq $main_pkey);

            # let's chop the row into subrows;
        foreach my $table (@tables) {
            for ( keys %$base_row ) {
                if ( $_ =~ /$table\_(.*)$/ ) {
                    $data->{$main_pkey}->{$table} ->{ ($base_row->{ $table . '_id' } ||$main_pkey )}->{$1} = $base_row->{$_};
                }
            }
        }

    }

    # For related "record" values, we can simply prepopulate the
    # Jifty::DBI::Record cache and life will be good. (I suspect we want
    # to do this _before_ doing the initial primary record load on the
    # off chance that the primary record will try to do the relevant
    # prefetch manually For related "collection" values, our job is a bit
    # harder. we need to create a new empty collection object, set it's
    # "must search" to 0 and manually add the records to it for each of
    # the items we find. Then we need to ram it into place.

    foreach my $row_id ( @order) {
        my $item;
        foreach my $row ( values %{ $data->{$row_id}->{'main'} } ) {
            $item = $self->new_item();
            $item->load_from_hash($row);
        }
        foreach my $alias ( grep { $_ ne 'main' } keys %{ $data->{$row_id} } ) {

            my $related_rows = $data->{$row_id}->{$alias};
            my ( $class, $col_name ) = $self->class_and_column_for_alias($alias);
            if ($class) {

            if ( $class->isa('Jifty::DBI::Collection') ) {
                my $collection = $class->new( handle => $self->_handle );
                foreach my $row( sort { $a->{id} <=> $b->{id} }  values %$related_rows ) {
                    my $entry
                        = $collection->new_item( handle => $self->_handle );
                    $entry->load_from_hash($row);
                    $collection->add_record($entry);
                }

                $item->_prefetched_collection( $col_name => $collection );
            } elsif ( $class->isa('Jifty::DBI::Record') ) {
                foreach my $related_row ( values %$related_rows ) {
                    my $item = $class->new( handle => $self->_handle );
                    $item->load_from_hash($related_row);
                }
            } else {
                Carp::cluck(
                    "Asked to preload $alias as a $class. Don't know how to handle $class"
                );
            }
            }


        }
        $self->add_record($item);

    }
    if ( $records->err ) {
        $self->{'must_redo_search'} = 0;
    }

    return $self->_record_count;
}

=head2 add_record RECORD

Adds a record object to this collection.

This method automatically sets our "must redo search" flag to 0 and our "we have limits" flag to 1.

Without those two flags, counting the number of items wouldn't work.

=cut

sub add_record {
    my $self   = shift;
    my $record = shift;
    $self->_is_limited(1);
    $self->{'must_redo_search'} = 0;
    push @{ $self->{'items'} }, $record;
}

=head2 _record_count

This private internal method returns the number of
L<Jifty::DBI::Record> objects saved as a result of the last query.

=cut

sub _record_count {
    my $self = shift;
    return 0 unless defined $self->{'items'};
    return scalar @{ $self->{'items'} };
}

=head2 _do_count

This internal private method actually executes a counting operation on
the database; it is used by L</count> and L</count_all>.

=cut

sub _do_count {
    my $self = shift;
    my $all  = shift || 0;

    my $query_string = $self->build_select_count_query();
    my $records      = $self->_handle->simple_query($query_string);
    return 0 unless $records;

    my @row = $records->fetchrow_array();
    return 0 if $records->err;

    $self->{ $all ? 'count_all' : 'raw_rows' } = $row[0];

    return ( $row[0] );
}

=head2 _apply_limits STATEMENTREF

This routine takes a reference to a scalar containing an SQL
statement.  It massages the statement to limit the returned rows to
only C<< $self->rows_per_page >> rows, skipping C<< $self->first_row >>
rows.  (That is, if rows are numbered starting from 0, row number
C<< $self->first_row >> will be the first row returned.)  Note that it
probably makes no sense to set these variables unless you are also
enforcing an ordering on the rows (with L</order_by_cols>, say).

=cut

sub _apply_limits {
    my $self         = shift;
    my $statementref = shift;
    $self->_handle->apply_limits( $statementref, $self->rows_per_page,
        $self->first_row );

}

=head2 _distinct_query STATEMENTREF

This routine takes a reference to a scalar containing an SQL
statement.  It massages the statement to ensure a distinct result set
is returned.

=cut

sub _distinct_query {
    my $self         = shift;
    my $statementref = shift;
    $self->_handle->distinct_query( $statementref, $self );
}

=head2 _build_joins

Build up all of the joins we need to perform this query.

=cut

sub _build_joins {
    my $self = shift;

    return ( $self->_handle->_build_joins($self) );

}

=head2 _is_joined 

Returns true if this collection will be joining multiple tables
together.

=cut

sub _is_joined {
    my $self = shift;
    if ( $self->{'leftjoins'} && keys %{ $self->{'leftjoins'} } ) {
        return (1);
    } else {
        return ( @{ $self->{'aliases'} } );
    }

}

# LIMIT clauses are used for restricting ourselves to subsets of the
# search.
sub _limit_clause {
    my $self = shift;
    my $limit_clause;

    if ( $self->rows_per_page ) {
        $limit_clause = " LIMIT ";
        if ( $self->first_row != 0 ) {
            $limit_clause .= $self->first_row . ", ";
        }
        $limit_clause .= $self->rows_per_page;
    } else {
        $limit_clause = "";
    }
    return $limit_clause;
}

=head2 _is_limited

If we've limited down this search, return true. Otherwise, return
false.

=cut

sub _is_limited {
    my $self = shift;
    if (@_) {
        $self->{'is_limited'} = shift;
    } else {
        return ( $self->{'is_limited'} );
    }
}

=head2 build_select_query

Builds a query string for a "SELECT rows from Tables" statement for
this collection

=cut

sub build_select_query {
    my $self = shift;

    # The initial SELECT or SELECT DISTINCT is decided later

    my $query_string = $self->_build_joins . " ";

    if ( $self->_is_limited ) {
        $query_string .= $self->_where_clause . " ";
    }
    if ( $self->distinct_required ) {

        # DISTINCT query only required for multi-table selects
        $self->_distinct_query( \$query_string );
    } else {
        $query_string
            = "SELECT " . $self->_preload_columns . " FROM $query_string";
        $query_string .= $self->_group_clause;
        $query_string .= $self->_order_clause;
    }

    $self->_apply_limits( \$query_string );

    return ($query_string)

}

=head2 preload_columns

The columns that the query would load for result items.  By default it's everything.

XXX TODO: in the case of distinct, it needs to work as well.

=cut

sub _preload_columns {
    my $self = shift;

    my @cols            = ();
    my $item            = $self->new_item;
    if( $self->{columns} and @{ $self->{columns} } ) {
         push @cols, @{$self->{columns}};
         # push @cols, map { warn "Preloading $_"; "main.$_ as main_" . $_ } @{$preload_columns};
    } else {
        push @cols, $self->_qualified_record_columns( 'main' => $item );
    }
    my %preload_related = %{ $self->preload_related || {} };
    foreach my $alias ( keys %preload_related ) {
        my $related_obj = $preload_related{$alias};
        if ( my $col_obj = $item->column($related_obj) ) {
            my $reference_type = $col_obj->refers_to;

            my $reference_item;

            if ( !$reference_type ) {
                Carp::cluck(
                    "Asked to prefetch $col_obj->name for $self. But $col_obj->name isn't a known reference"
                );
            } elsif ( $reference_type->isa('Jifty::DBI::Collection') ) {
                $reference_item = $reference_type->new->new_item();
            } elsif ( $reference_type->isa('Jifty::DBI::Record') ) {
                $reference_item = $reference_type->new;
            } else {
                Carp::cluck(
                    "Asked to prefetch $col_obj->name for $self. But $col_obj->name isn't a known type"
                );
            }

            push @cols,
                $self->_qualified_record_columns( $alias => $reference_item );
        }

   #     push @cols, map { $_ . ".*" } keys %{ $self->preload_related || {} };

    }
    return CORE::join( ', ', @cols );
}

=head2 class_and_column_for_alias

Takes the alias you've assigned to a prefetched related object. Returns the class
of the column we've declared that alias preloads.

=cut

sub class_and_column_for_alias {
    my $self            = shift;
    my $alias           = shift;
    my %preload_related = %{ $self->preload_related || {} };
    my $related_colname = $preload_related{$alias};
    if ( my $col_obj = $self->new_item->column($related_colname) ) {
        return ( $col_obj->refers_to => $related_colname );
    }
    return undef;
}

sub _qualified_record_columns {
    my $self  = shift;
    my $alias = shift;
    my $item  = shift;
    grep {$_} map {
        my $col = $_;
        if ( $col->virtual ) {
            undef;
        } else {
            $col = $col->name;
            $alias . "." . $col . " as " . $alias . "_" . $col;
        }
    } $item->columns;
}

=head2  prefetch ALIAS_NAME ATTRIBUTE

prefetches all related rows from alias ALIAS_NAME into the record attribute ATTRIBUTE of the
sort of item this collection is.

If you have employees who have many phone numbers, this method will let you search for all your employees
    and prepopulate their phone numbers.

Right now, in order to make this work, you need to do an explicit join between your primary table and the subsidiary tables AND then specify the name of the attribute you want to prefetch related data into.
This method could be a LOT smarter. since we already know what the relationships between our tables are, that could all be precomputed.

XXX TODO: in the future, this API should be extended to let you specify columns.

=cut

sub prefetch {
    my $self           = shift;
    my $alias          = shift;
    my $into_attribute = shift;

    my $preload_related = $self->preload_related() || {};

    $preload_related->{$alias} = $into_attribute;

    $self->preload_related($preload_related);

}

=head2 distinct_required

Returns true if Jifty::DBI expects that this result set will end up
with repeated rows and should be "condensed" down to a single row for
each unique primary key.

Out of the box, this method returns true if you've joined to another table.
To add additional logic, feel free to override this method in your subclass.

XXX TODO: it should be possible to create a better heuristic than the simple
"is it joined?" question we're asking now. Something along the lines of "are we
joining this table to something that is not the other table's primary key"

=cut

sub distinct_required {
    my $self = shift;
    return( $self->_is_joined ? 1 : 0 );
}

=head2 build_select_count_query

Builds a SELECT statement to find the number of rows this collection
 would find.

=cut

sub build_select_count_query {
    my $self = shift;

    my $query_string = $self->_build_joins . " ";

    if ( $self->_is_limited ) {
        $query_string .= $self->_where_clause . " ";
    }

    # DISTINCT query only required for multi-table selects
    if ( $self->_is_joined ) {
        $query_string = $self->_handle->distinct_count( \$query_string );
    } else {
        $query_string = "SELECT count(main.id) FROM " . $query_string;
    }

    return ($query_string);
}

=head2 do_search

C<Jifty::DBI::Collection> usually does searches "lazily". That is, it
does a C<SELECT COUNT> or a C<SELECT> on the fly the first time you ask
for results that would need one or the other.  Sometimes, you need to
display a count of results found before you iterate over a collection,
but you know you're about to do that too. To save a bit of wear and tear
on your database, call C<do_search> before that C<count>.

=cut

sub do_search {
    my $self = shift;
    $self->_do_search() if $self->{'must_redo_search'};

}

=head2 next

Returns the next row from the set as an object of the type defined by
sub new_item.  When the complete set has been iterated through,
returns undef and resets the search such that the following call to
L</next> will start over with the first item retrieved from the
database.

=cut

sub next {
    my $self = shift;

    my $item = $self->peek;

    if ( $self->{'itemscount'} < $self->_record_count )
    {
        $self->{'itemscount'}++;
    } else {    #we've gone through the whole list. reset the count.
        $self->goto_first_item();
    }

    return ($item);
}

=head2 peek

Exactly the same as next, only it doesn't move the iterator.

=cut

sub peek {
    my $self = shift;

    return (undef) unless ( $self->_is_limited );

    $self->_do_search() if $self->{'must_redo_search'};

    if ( $self->{'itemscount'} < $self->_record_count )
    {    #return the next item
        my $item = ( $self->{'items'}[ $self->{'itemscount'} ] );
        return ($item);
    } else {    #no more items!
        return (undef);
    }
}

=head2 goto_first_item

Starts the recordset counter over from the first item. The next time
you call L</next>, you'll get the first item returned by the database,
as if you'd just started iterating through the result set.

=cut

sub goto_first_item {
    my $self = shift;
    $self->goto_item(0);
}

=head2 goto_item

Takes an integer, n.  Sets the record counter to n. the next time you
call L</next>, you'll get the nth item.

=cut

sub goto_item {
    my $self = shift;
    my $item = shift;
    $self->{'itemscount'} = $item;
}

=head2 first

Returns the first item

=cut

sub first {
    my $self = shift;
    $self->goto_first_item();
    return ( $self->next );
}

=head2 last

Returns the last item

=cut

sub last {
    my $self = shift;
    $self->goto_item( ( $self->count ) - 1 );
    return ( $self->next );
}

=head2 items_array_ref

Return a reference to an array containing all objects found by this
search.

=cut

sub items_array_ref {
    my $self = shift;

    # If we're not limited, return an empty array
    return [] unless $self->_is_limited;

    # Do a search if we need to.
    $self->_do_search() if $self->{'must_redo_search'};

    # If we've got any items in the array, return them.  Otherwise,
    # return an empty array
    return ( $self->{'items'} || [] );
}

=head2 new_item

Should return a new object of the correct type for the current collection.
Must be overridden by a subclassed.

=cut

sub new_item {
    my $self  = shift;
    my $class = $self->record_class();

    die "Jifty::DBI::Collection needs to be subclassed; override new_item\n"
        unless $class;

    $class->require();
    return $class->new( handle => $self->_handle );
}

=head2 record_class

Returns the record class which this is a collection of; override this
to subclass.  Or, pass it the name of a class an an argument after
creating a C<Jifty::DBI::Collection> object to create an 'anonymous'
collection class.

If you haven't specified a record class, this returns a best guess at
the name of the record class for this collection.

It uses a simple heuristic to determine the record class name -- It
chops "Collection" off its own name. If you want to name your records
and collections differently, go right ahead, but don't say we didn't
warn you.

=cut

sub record_class {
    my $self = shift;
    if (@_) {
        $self->{record_class} = shift if (@_);
        $self->{record_class} = ref $self->{record_class}
            if ref $self->{record_class};
    } elsif ( not $self->{record_class} ) {
        my $class = ref($self);
        $class =~ s/Collection$//
            or die "Can't guess record class from $class";
        $self->{record_class} = $class;
    }
    return $self->{record_class};
}

=head2 redo_search

Takes no arguments.  Tells Jifty::DBI::Collection that the next time
it's asked for a record, it should requery the database

=cut

sub redo_search {
    my $self = shift;
    $self->{'must_redo_search'} = 1;
    delete $self->{$_} for qw(items raw_rows count_all);
    $self->{'itemscount'} = 0;
}

=head2 unlimit

Clears all restrictions and causes this object to return all
rows in the primary table.

=cut

sub unlimit {
    my $self = shift;

    $self->clean_slate();
    $self->_is_limited(-1);
}

=head2 limit

Takes a hash of parameters with the following keys:

=over 4

=item table 

Can be set to something different than this table if a join is
wanted (that means we can't do recursive joins as for now).  

=item alias

Unless alias is set, the join criterias will be taken from EXT_LINKcolumn
and INT_LINKcolumn and added to the criterias.  If alias is set, new
criterias about the foreign table will be added.

=item column

Column to be checked against.

=item value

Should always be set and will always be quoted.  If the value is a
subclass of Jifty::DBI::Object, the value will be interpreted to be
the object's id.

=item operator

operator is the SQL operator to use for this phrase.  Possible choices include:

=over 4

=item "="

=item "!="

Any other standard SQL comparision operators that your underlying
database supports are also valid.

=item "LIKE"

=item "NOT LIKE"

=item "MATCHES"

MATCHES is like LIKE, except it surrounds the value with % signs.

=item "STARTSWITH"

STARTSWITH is like LIKE, except it only appends a % at the end of the string

=item "ENDSWITH"

ENDSWITH is like LIKE, except it prepends a % to the beginning of the string

=back

=item entry_aggregator 

Can be AND or OR (or anything else valid to aggregate two clauses in SQL)

=item case_sensitive

on some databases, such as postgres, setting case_sensitive to 1 will make
this search case sensitive.  Note that this flag is ignored if the column
is numeric.

=back

=cut 

sub limit {
    my $self = shift;
    my %args = (
        table            => $self->table,
        column           => undef,
        value            => undef,
        alias            => undef,
        quote_value      => 1,
        entry_aggregator => 'or',
        case_sensitive   => undef,
        operator         => '=',
        subclause        => undef,
        leftjoin         => undef,
        @_    # get the real argumentlist
    );

    # We need to be passed a column and a value, at very least
    croak "Must provide a column to limit"
        unless defined $args{column};
    croak "Must provide a value to limit to"
        unless defined $args{value};

    # make passing in an object DTRT
    if ( ref( $args{value} ) && $args{value}->isa('Jifty::DBI::Record') ) {
        $args{value} = $args{value}->id;
    }

    #since we're changing the search criteria, we need to redo the search
    $self->redo_search();

    if ( $args{'column'} ) {

        #If it's a like, we supply the %s around the search term
        if ( $args{'operator'} =~ /MATCHES/i ) {
            $args{'value'}    = "%" . $args{'value'} . "%";
        } elsif ( $args{'operator'} =~ /STARTSWITH/i ) {
            $args{'value'}    = $args{'value'} . "%";
        } elsif ( $args{'operator'} =~ /ENDSWITH/i ) {
            $args{'value'}    = "%" . $args{'value'};
        }
        $args{'operator'} =~ s/(?:MATCHES|ENDSWITH|STARTSWITH)/LIKE/i;

        #if we're explicitly told not to to quote the value or
        # we're doing an IS or IS NOT (null), don't quote the operator.

        if ( $args{'quote_value'} && $args{'operator'} !~ /IS/i ) {
            my $tmp = $self->_handle->dbh->quote( $args{'value'} );

            # Accomodate DBI drivers that don't understand UTF8
            if ( $] >= 5.007 ) {
                require Encode;
                if ( Encode::is_utf8( $args{'value'} ) ) {
                    Encode::_utf8_on($tmp);
                }
            }
            $args{'value'} = $tmp;
        }
    }

    #TODO: $args{'value'} should take an array of values and generate
    # the proper where clause.

    #If we're performing a left join, we really want the alias to be the
    #left join criterion.

    if (   ( defined $args{'leftjoin'} )
        && ( not defined $args{'alias'} ) )
    {
        $args{'alias'} = $args{'leftjoin'};
    }

    # {{{ if there's no alias set, we need to set it

    unless ( $args{'alias'} ) {

        #if the table we're looking at is the same as the main table
        if ( $args{'table'} eq $self->table ) {

            # TODO this code assumes no self joins on that table.
            # if someone can name a case where we'd want to do that,
            # I'll change it.

            $args{'alias'} = 'main';
        }

        else {
            $args{'alias'} = $self->new_alias( $args{'table'} );
        }
    }

    # }}}

    # Set this to the name of the column and the alias, unless we've been
    # handed a subclause name

    my $qualified_column = $args{'alias'} . "." . $args{'column'};
    my $clause_id = $args{'subclause'} || $qualified_column;

    # If we're trying to get a leftjoin restriction, lets set
    # $restriction to point htere. otherwise, lets construct normally

    my $restriction;
    if ( $args{'leftjoin'} ) {
        $restriction = $self->{'leftjoins'}{ $args{'leftjoin'} }{'criteria'}
            { $clause_id } ||= [];
    } else {
        $restriction = $self->{'restrictions'}{ $clause_id } ||= [];
    }

    # If it's a new value or we're overwriting this sort of restriction,

    if (   $self->_handle->case_sensitive
        && defined $args{'value'}
        && $args{'quote_value'}
        && !$args{'case_sensitive'} )
    {

        # don't worry about case for numeric columns_in_db
        my $column_obj = $self->new_item()->column( $args{column} );
        if ( defined $column_obj ? $column_obj->is_string : 1 ) {
            ( $qualified_column, $args{'operator'}, $args{'value'} )
                = $self->_handle->_make_clause_case_insensitive(
                $qualified_column, $args{'operator'}, $args{'value'} );
        }
    }

    my $clause = {
        column   => $qualified_column,
        operator => $args{'operator'},
        value    => $args{'value'},
    };

    # Juju because this should come _AFTER_ the EA
    my @prefix;
    if ( $self->{'_open_parens'}{ $clause_id } ) {
        @prefix = ('(') x delete $self->{'_open_parens'}{ $clause_id };
    }

    if ( lc( $args{'entry_aggregator'} || "" ) eq 'none' || !@$restriction ) {
        @$restriction = (@prefix, $clause);
    } else {
        push @$restriction, $args{'entry_aggregator'}, @prefix , $clause;
    }

    # We're now limited. people can do searches.

    $self->_is_limited(1);

    if ( defined( $args{'alias'} ) ) {
        return ( $args{'alias'} );
    } else {
        return (1);
    }
}

=head2 open_paren CLAUSE

Places an open paren at the current location in the given C<CLAUSE>.
Note that this can be used for Deep Magic, and has a high likelyhood
of allowing you to construct malformed SQL queries.  Its interface
will probably change in the near future, but its presence allows for
arbitrarily complex queries.

=cut

sub open_paren {
    my ( $self, $clause ) = @_;
    $self->{_open_parens}{$clause}++;
}

=head2 close_paren CLAUSE

Places a close paren at the current location in the given C<CLAUSE>.
Note that this can be used for Deep Magic, and has a high likelyhood
of allowing you to construct malformed SQL queries.  Its interface
will probably change in the near future, but its presence allows for
arbitrarily complex queries.

=cut

# Immediate Action
sub close_paren {
    my ( $self, $clause ) = @_;
    my $restriction = $self->{'restrictions'}{ $clause } ||= [];
    push @$restriction, ')';
}

sub _add_subclause {
    my $self      = shift;
    my $clauseid  = shift;
    my $subclause = shift;

    $self->{'subclauses'}{"$clauseid"} = $subclause;

}

sub _where_clause {
    my $self         = shift;
    my $where_clause = '';

    # Go through all the generic restrictions and build up the
    # "generic_restrictions" subclause.  That's the only one that the
    # collection builds itself.  Arguably, the abstraction should be
    # better, but I don't really see where to put it.
    $self->_compile_generic_restrictions();

    #Go through all restriction types. Build the where clause from the
    #Various subclauses.

    my @subclauses = grep defined && length, values %{ $self->{'subclauses'} };

    $where_clause = " WHERE " . CORE::join( ' AND ', @subclauses )
        if (@subclauses);

    return ($where_clause);

}

#Compile the restrictions to a WHERE Clause

sub _compile_generic_restrictions {
    my $self = shift;

    delete $self->{'subclauses'}{'generic_restrictions'};

    # Go through all the restrictions of this type. Buld up the generic subclause
    my $result = '';
    foreach my $restriction ( grep $_ && @$_, values %{ $self->{'restrictions'} } ) {
        $result .= ' AND ' if $result;
        $result .= '(';
        foreach my $entry ( @$restriction ) {
            unless ( ref $entry ) {
                $result .= ' '. $entry . ' ';
            }
            else {
                $result .= join ' ', @{$entry}{qw(column operator value)};
            }
        }
        $result .= ')';
    }
    return ($self->{'subclauses'}{'generic_restrictions'} = $result);
}

# set $self->{$type .'_clause'} to new value
# redo_search only if new value is really new
sub _set_clause {
    my $self = shift;
    my ( $type, $value ) = @_;
    $type .= '_clause';
    if ( ( $self->{$type} || '' ) ne ( $value || '' ) ) {
        $self->redo_search;
    }
    $self->{$type} = $value;
}

=head2 order_by_cols DEPRECATED

*DEPRECATED*. Use C<order_by> method.

=cut

sub order_by_cols {
    require Carp;
    Carp::cluck("order_by_cols is deprecated, use order_by method");
    goto &order_by;
}

=head2 order_by EMPTY|HASH|ARRAY_OF_HASHES

Orders the returned results by column(s) and/or function(s) on column(s).

Takes a paramhash of C<alias>, C<column> and C<order>
or C<function> and C<order>.
C<alias> defaults to main.
C<order> defaults to ASC(ending), DES(cending) is also a valid value.
C<column> and C<function> have no default values.

Use C<function> instead of C<alias> and C<column> to order by
the function value. Note that if you want use a column as argument of
the function then you have to build correct reference with alias
in the C<alias.column> format.

Use array of hashes to order by many columns/functions.

The results would be unordered if method called without arguments.

Returns the current list of columns.

=cut

sub order_by {
    my $self = shift;
    if (@_) {
        my @args = @_;

        unless ( UNIVERSAL::isa( $args[0], 'HASH' ) ) {
            @args = {@args};
        }
        $self->{'order_by'} = \@args;
        $self->redo_search();
    }
    return ( $self->{'order_by'} || []);
}

=head2 _order_clause

returns the ORDER BY clause for the search.

=cut

sub _order_clause {
    my $self = shift;

    return '' unless $self->{'order_by'};

    my $clause = '';
    foreach my $row ( @{ $self->{'order_by'} } ) {

        my %rowhash = (
            alias  => 'main',
            column => undef,
            order  => 'ASC',
            %$row
        );
        if ( $rowhash{'order'} =~ /^des/i ) {
            $rowhash{'order'} = "DESC";
        } else {
            $rowhash{'order'} = "ASC";
        }

        if ( $rowhash{'function'} ) {
            $clause .= ( $clause ? ", " : " " );
            $clause .= $rowhash{'function'} . ' ';
            $clause .= $rowhash{'order'};

        } elsif ( (defined $rowhash{'alias'} )
            and ( $rowhash{'column'} ) )
        {

            $clause .= ( $clause ? ", " : " " );
            $clause .= $rowhash{'alias'} . "." if $rowhash{'alias'};
            $clause .= $rowhash{'column'} . " ";
            $clause .= $rowhash{'order'};
        }
    }
    $clause = " ORDER BY$clause " if $clause;
    return $clause;
}

=head2 group_by_cols DEPRECATED

*DEPRECATED*. Use group_by method.

=cut

sub group_by_cols {
    require Carp;
    Carp::cluck("group_by_cols is deprecated, use group_by method");
    goto &group_by;
}

=head2 group_by EMPTY|HASH|ARRAY_OF_HASHES

Groups the search results by column(s) and/or function(s) on column(s).

Takes a paramhash of C<alias> and C<column> or C<function>.
C<alias> defaults to main.
C<column> and C<function> have no default values.

Use C<function> instead of C<alias> and C<column> to group by
the function value. Note that if you want use a column as argument
of the function then you have to build correct reference with alias
in the C<alias.column> format.

Use array of hashes to group by many columns/functions.

The method is EXPERIMENTAL and subject to change.

=cut

sub group_by {
    my $self = shift;

    my @args = @_;

    unless ( UNIVERSAL::isa( $args[0], 'HASH' ) ) {
        @args = {@args};
    }
    $self->{'group_by'} = \@args;
    $self->redo_search();
}

=head2 _group_clause

Private function to return the "GROUP BY" clause for this query.

=cut

sub _group_clause {
    my $self = shift;
    return '' unless $self->{'group_by'};

    my $row;
    my $clause;

    foreach $row ( @{ $self->{'group_by'} } ) {
        my %rowhash = (
            alias => 'main',

            column => undef,
            %$row
        );
        if ( $rowhash{'function'} ) {
            $clause .= ( $clause ? ", " : " " );
            $clause .= $rowhash{'function'};

        } elsif ( ( $rowhash{'alias'} )
            and ( $rowhash{'column'} ) )
        {

            $clause .= ( $clause ? ", " : " " );
            $clause .= $rowhash{'alias'} . ".";
            $clause .= $rowhash{'column'};
        }
    }
    if ($clause) {
        return " GROUP BY" . $clause . " ";
    } else {
        return '';
    }
}

=head2 new_alias table_OR_CLASS

Takes the name of a table or a Jifty::DBI::Record subclass.
Returns the string of a new Alias for that table, which can be used 
to Join tables or to limit what gets found by
a search.

=cut

sub new_alias {
    my $self = shift;
    my $refers_to = shift || die "Missing parameter";
    my $table;

    if ( $refers_to->can('table') ) {
        $table = $refers_to->table;
    } else {
        $table = $refers_to;
    }

    my $alias = $self->_get_alias($table);

    my $subclause = "$table $alias";

    push( @{ $self->{'aliases'} }, $subclause );

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

=head2 join

Join instructs Jifty::DBI::Collection to join two tables.  

The standard form takes a param hash with keys C<alias1>, C<column1>, C<alias2>
and C<column2>. C<alias1> and C<alias2> are column aliases obtained from
$self->new_alias or a $self->limit. C<column1> and C<column2> are the columns 
in C<alias1> and C<alias2> that should be linked, respectively.  For this
type of join, this method has no return value.

Supplying the parameter C<type> => 'left' causes Join to perform a left
join.  in this case, it takes C<alias1>, C<column1>, C<table2> and
C<column2>. Because of the way that left joins work, this method needs a
table for the second column rather than merely an alias.  For this type
of join, it will return the alias generated by the join.

The parameter C<operator> defaults C<=>, but you can specify other
operators to join with.

Instead of C<alias1>/C<column1>, it's possible to specify expression, to join
C<alias2>/C<table2> on an arbitrary expression.

=cut

sub join {
    my $self = shift;
    my %args = (
        type    => 'normal',
        column1 => undef,
        alias1  => 'main',
        table2  => undef,
        column2 => undef,
        alias2  => undef,
        @_
    );

    $self->_handle->join( collection => $self, %args );

}

=head2 set_page_info [per_page => NUMBER,] [current_page => NUMBER]

Sets the current page (one-based) and number of items per page on the
pager object, and pulls the number of elements from the collection.
This both sets up the collection's L<Data::Page> object so that you
can use its calculations, and sets the L<Jifty::DBI::Collection>
C<first_row> and C<rows_per_page> so that queries return values from
the selected page.

=cut

sub set_page_info {
    my $self = shift;
    my %args = (
        per_page     => undef,
        current_page => undef,    # 1-based
        @_
    );

    $self->pager->total_entries( $self->count_all )
        ->entries_per_page( $args{'per_page'} )
        ->current_page( $args{'current_page'} );

    $self->rows_per_page( $args{'per_page'} );
    $self->first_row( $self->pager->first || 1 );

}

=head2 rows_per_page

limits the number of rows returned by the database.  Optionally, takes
an integer which restricts the # of rows returned in a result Returns
the number of rows the database should display.

=cut

sub rows_per_page {
    my $self = shift;
    $self->{'show_rows'} = shift if (@_);

    return ( $self->{'show_rows'} );
}

=head2 first_row

Get or set the first row of the result set the database should return.
Takes an optional single integer argrument. Returns the currently set
integer first row that the database should return.


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

=head2 count

Returns the number of records in the set.

=cut

sub count {
    my $self = shift;

    # An unlimited search returns no tickets
    return 0 unless ( $self->_is_limited );

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
# 22:28 [Robrt(500@outer.space)] IIRC, if you do a search with a limit, then raw_rows will == limit.
# 22:31 [msg(Robrt)] ah.
# 22:31 [msg(Robrt)] that actually makes sense
# 22:31 [Robrt(500@outer.space)] You should paste this conversation into the count_all docs.
# 22:31 [msg(Robrt)] perhaps I'll create a new method that _actually_ do that.
# 22:32 [msg(Robrt)] since I'm not convinced it's been doing that correctly

sub count_all {
    my $self = shift;

    # An unlimited search returns no tickets
    return 0 unless ( $self->_is_limited );

    # If we haven't actually got all objects loaded in memory, we
    # really just want to do a quick count from the database.
    if ( $self->{'must_redo_search'} || !$self->{'count_all'} ) {

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

=head2 is_last

Returns true if the current row is the last record in the set.

=cut

sub is_last {
    my $self = shift;

    return undef unless $self->count;

    if ( $self->_items_counter == $self->count ) {
        return (1);
    } else {
        return (0);
    }
}

=head2 DEBUG

Gets/sets the DEBUG flag.

=cut

sub DEBUG {
    my $self = shift;
    if (@_) {
        $self->{'DEBUG'} = shift;
    }
    return ( $self->{'DEBUG'} );
}

=head2 column

Normally a collection object contains record objects populated with all columns
in the database, but you can restrict the records to only contain some
particular columns, by calling the C<column> method once for each column you
are interested in.

Takes a hash of parameters; the C<column>, C<table> and C<alias> keys means
the same as in the C<limit> method.  A special C<function> key may contain
one of several possible kinds of expressions:

=over 4

=item C<DISTINCT COUNT>

Same as C<COUNT(DISTINCT ?)>.

=item Expression with C<?> in it

The C<?> is substituted with the column name, then passed verbatim to the
underlying C<SELECT> statement.

=item Expression with C<(> in it

The expression is passed verbatim to the underlying C<SELECT>.

=item Any other expression

The expression is taken to be a function name.  For example, C<SUM> means
the same thing as C<SUM(?)>.

=back

=cut

sub column {
    my $self = shift;
    my %args = (
        table    => undef,
        alias    => undef,
        column   => undef,
        function => undef,
        @_
    );

    my $table = $args{table} || do {
        if ( my $alias = $args{alias} ) {
            $alias =~ s/_\d+$//;
            $alias;
        } else {
            $self->table;
        }
    };

    my $name = ( $args{alias} || 'main' ) . '.' . $args{column};
    if ( my $func = $args{function} ) {
        if ( $func =~ /^DISTINCT\s*COUNT$/i ) {
            $name = "COUNT(DISTINCT $name)";
        }

        # If we want to substitute
        elsif ( $func =~ /\?/ ) {
            $name =~ s/\?/$name/g;
        }

        # If we want to call a simple function on the column
        elsif ( $func !~ /\(/ ) {
            $name = "\U$func\E($name)";
        } else {
            $name = $func;
        }

    }

    my $column = "col" . @{ $self->{columns} ||= [] };
    $column = $args{column} if $table eq $self->table and !$args{alias};
    $column = ($args{'alias'}||'main')."_".$column;
    push @{ $self->{columns} }, "$name AS \L$column";
    return $column;
}

=head2 columns LIST

Specify that we want to load only the columns in LIST, which is a 

=cut

sub columns {
    my $self = shift;
    $self->column( column => $_ ) for @_;
}

=head2 columns_in_db table

Return a list of columns in table, lowercased.

TODO: Why are they lowercased?

=cut

sub columns_in_db {
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

=head2 has_column  { table => undef, column => undef }

Returns true if table has column column.
Return false otherwise

=cut

sub has_column {
    my $self = shift;
    my %args = (
        column => undef,
        table  => undef,
        @_
    );

    my $table  = $args{table}  or die;
    my $column = $args{column} or die;
    return grep { $_ eq $column } $self->columns_in_db($table);
}

=head2 table [table]

If called with an argument, sets this collection's table.

Always returns this collection's table.

=cut

sub table {
    my $self = shift;
    $self->{table} = shift if (@_);
    return $self->{table};
}

=head2 clone

Returns copy of the current object with all search restrictions.

=cut

sub clone {
    my $self = shift;

    my $obj = bless {}, ref($self);
    %$obj = %$self;

    $obj->redo_search();    # clean out the object of data

    $obj->{$_} = Clone::clone( $obj->{$_} ) for
        grep exists $self->{ $_ }, $self->_cloned_attributes;
    return $obj;
}

=head2 _cloned_attributes

Returns list of the object's fields that should be copied.

If your subclass store references in the object that should be copied while
clonning then you probably want override this method and add own values to
the list.

=cut

sub _cloned_attributes {
    return qw(
        aliases
        leftjoins
        subclauses
        restrictions
    );
}

1;
__END__



=head1 TESTING

In order to test most of the features of C<Jifty::DBI::Collection>,
you need to provide C<make test> with a test database.  For each DBI
driver that you would like to test, set the environment variables
C<JDBI_TEST_FOO>, C<JDBI_TEST_FOO_USER>, and C<JDBI_TEST_FOO_PASS> to a
database name, database username, and database password, where "FOO"
is the driver name in all uppercase.  You can test as many drivers as
you like.  (The appropriate C<DBD::> module needs to be installed in
order for the test to work.)  Note that the C<SQLite> driver will
automatically be tested if C<DBD::Sqlite> is installed, using a
temporary file as the database.  For example:

  JDBI_TEST_MYSQL=test JDBI_TEST_MYSQL_USER=root JDBI_TEST_MYSQL_PASS=foo \
    JDBI_TEST_PG=test JDBI_TEST_PG_USER=postgres  make test


=head1 AUTHOR

Copyright (c) 2001-2005 Jesse Vincent, jesse@fsck.com.

All rights reserved.

This library is free software; you can redistribute it
and/or modify it under the same terms as Perl itself.


=head1 SEE ALSO

Jifty::DBI::Handle, Jifty::DBI::Record.

=cut

