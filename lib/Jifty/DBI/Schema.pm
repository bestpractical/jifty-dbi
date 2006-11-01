use warnings;
use strict;

package Jifty::DBI::Schema;

=head1 NAME

Jifty::DBI::Schema - Use a simple syntax to describe a Jifty table.

=head1 SYNOPSIS

    package Wifty::Model::Page;
    use Jifty::DBI::Schema;
    use Jifty::DBI::Record schema {
    # ... your columns here ...
    };

=cut

=head1 DESCRIPTION

Each Jifty Application::Model::Class module describes a record class
for a Jifty application.  Each C<column> statement sets out the name
and attributes used to describe the column in a backend database, in
user interfaces, and other contexts.  For example:

    column content =>
       type is 'text',
       label is 'Content',
       render as 'textarea';

defines a column called C<content> that is of type C<text>.  It will be
rendered with the label C<Content> (note the capital) and as a C<textarea> in
a HTML form.

Jifty::DBI::Schema builds a L<Jifty::DBI::Column>.  That class defines
other attributes for database structure that are not exposed directly
here.  One example of this is the "refers_to" method used to create
associations between classes.

=cut

use Carp qw/croak carp/;
use Exporter::Lite;
our @EXPORT
    = qw(column type default literal validator immutable unreadable length distinct mandatory not_null sort_order valid_values label hints render_as render since input_filters output_filters filters virtual is as by are on schema);

our $SCHEMA;
our $SORT_ORDERS = {};

=head1 FUNCTIONS

All these functions are exported.  However, if you use the C<schema> helper function,
they will be unimported at the end of the block passed to C<schema>.

=head2 schema

Takes a block with schema declarations.  Unimports all helper functions after
executing the code block.  Usually used at C<BEGIN> time via this idiom:

    use Jifty::DBI::Record schema { ... };

If your application subclasses C<::Record>, then write this instead:

    use MyApp::Record schema { ... };

=cut

sub schema (&) {
    my $code = shift;

    my $from = (caller)[0];

    my $new_code = sub {
        $code->();

        # Unimport all our symbols from the calling package.
        foreach my $sym (@EXPORT) {
            no strict 'refs';
            undef *{"$from\::$sym"}
                if \&{"$from\::$sym"} == \&$sym;
        }

        # Then initialize all columns
        foreach my $column (sort keys %{$from->COLUMNS||{}}) {
            $from->_init_methods_for_column($from->COLUMNS->{$column});
        }
    };

    return('-base' => $new_code);
}

=head2 column

Set forth the description of a column in the data store.

Note: If the column uses 'refers_to' to reference another class then you 
should not name the column ending in '_id' as it has special meaning.

=cut

sub column {
    my $name = lc(shift);

    my $from = (caller)[0];
    $from =~ s/::Schema//;

    croak "Base of schema class $from is not a Jifty::DBI::Record"
      unless UNIVERSAL::isa($from, "Jifty::DBI::Record");

    croak "Illegal column definition for column $name in $from"
      if grep {not UNIVERSAL::isa($_, "Jifty::DBI::Schema::Trait")} @_;

    $from->_init_columns;


    my @args = (
        ! unreadable(),
        ! immutable(),
        ! virtual(),
        type(''),
        @_
    );

    my $column = Jifty::DBI::Column->new( { name => $name } );
    $column->sort_order($SORT_ORDERS->{$from}++);

    $_->apply($column) for @args;

    if ( my $refclass = $column->refers_to ) {
        $refclass->require();
        $column->type('integer') unless ( $column->type );

        if ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Record' ) ) {
            if ( $name =~ /(.*)_id$/ ) {
                my $aliased_as = $1;
                my $virtual_column = $from->add_column($aliased_as);

                # XXX FIXME I think the next line is wrong, but things
                # explode without it -- mostly because we unique-key
                # on name instead of some conbination of name and
                # alias_for_column in a couple places
                $virtual_column->name( $name );
                $virtual_column->aliased_as($aliased_as);
                $_->apply($virtual_column) for @args;
                $column->refers_to(undef);
                $virtual_column->alias_for_column($name);
                $from->_init_methods_for_column($virtual_column);
            }
            $column->by('id') unless $column->by;
            $column->type('integer') unless $column->type;
        } elsif ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Collection' ) ) {
            $column->by('id') unless $column->by;
            $column->virtual('1');
        } else {
            warn "Error: $refclass neither Record nor Collection";
        }
    } else {
        $column->type('varchar(255)') unless $column->type;
    }


    $from->COLUMNS->{$name} = $column;

    # Heuristics: If we are called through Jifty::DBI::Schema, 
    # then we know that we are going to initialize methods later
    # through the &schema wrapper, so we defer initialization here
    # to not upset column names such as "label" and "type".
    # (We may not *have* a caller(1) if the user is executing a .pm file.)
    my $caller1 = caller(1);
    return if defined $caller1 && $caller1 eq __PACKAGE__;

    $from->_init_methods_for_column($column)
}

=head2 refers_to

Indicates that the column references an object or a collection of objects in another
class.  You may refer to either a class that inherits from Jifty::Record by a primary
key in that class or to a class that inherits from Jifty::Collection.

Correct usage is C<refers_to Application::Model::OtherClass by 'column_name'>, where
Application::Model::OtherClass is a valid Jifty model and C<'column_name'> is
a column containing unique values in OtherClass.  You can omit C<by 'column_name'> and
the column name 'id' will be used.

If you are referring to a Jifty::Collection then you must specify C<by 'column_name'>.

When accessing the value in the column the actual object referenced will be returned for
refernces to Jifty::Records and a reference to a Jifty::Collection will be returned for
columns referring to Jifty::Collections.

For columns referring to Jifty::Records you can access the actual value of the column
instead of the object reference by appending '_id' to the column name.  As a result,
you may not end any column name which uses 'refers_to' using '_id'.

=cut

=head2 type

type passed to our database abstraction layer, which should resolve it
to a database-specific type.  Correct usage is C<type is 'text'>.

Currently type is passed directly to the database.  There is no
intermediary mapping from abstract type names to database specific
types.

The impact of this is that not all column types are portable between
databases.  For example blobs have different names between
mysql and postgres.

=cut

sub type {
    _list( type => @_ );
}

=head2 default

Give a default value for the column.  Correct usage is C<default is
'foo'>.

=cut

sub default {
    _list( default => @_ );
}

=head2 literal

Used for default values, to connote that they should not be quoted
before being supplied as the default value for the column.  Correct
usage is C<default is literal 'now()'>.

=cut

sub literal($) {
    my $value = shift;
    return \$value;
}

=head2 validator

Defines a subroutine which returns a true value only for valid values
this column can have.  Correct usage is C<validator is \&foo>.

=cut

sub validator {
    _list( validator => @_ );
}

=head2 immutable

States that this column is not writable.  This is useful for
properties that are set at creation time but not modifiable
thereafter, like 'created by'.  Correct usage is C<is immutable>.

=cut

sub immutable {
    _item( writable => 0, @_ );
}

=head2 unreadable

States that this column is not directly readable by the application
using C<< $record->column >>; this is useful for password columns and
the like.  The data is still accessible via C<< $record->_value('') >>.
Correct usage is C<is unreadable>.

=cut

sub unreadable {
    _item( readable => 0, @_ );
}

=head2 length

Sets a maximum length to store in the database; values longer than
this are truncated before being inserted into the database, using
L<Jifty::DBI::Filter::Truncate>.  Note that this is in B<bytes>, not
B<characters>.  Correct usage is C<length is 42>.

=cut

sub length {
    _list( length => @_ );
}

=head2 mandatory

Mark as a required column.  May be used for generating user
interfaces.  Correct usage is C<is mandatory>.

=cut

sub mandatory {
    _item( mandatory => 1, @_ );
}

=head2 not_null

Same as L</mandatory>.  This is depricated.  Currect usage would be
C<is not_null>.

=cut

sub not_null {
    carp "'is not_null' is deprecated in favor of 'is mandatory'";
    _item( mandatory => 1, @_ );
}

=head2 distinct

Declares that a column should only have distinct values.  This
currently is implemented via database queries prior to updates
and creates instead of constraints on the database columns
themselves. This is because there is no support for distinct
columns implemented in L<DBIx::DBSchema> at this time.  
Correct usage is C<is distinct>.

=cut

sub distinct {
    _item( distinct => 1, @_ );
}

=head2 virtual

Declares that a column is not backed by an actual column in the
database, but is instead computed on-the-fly.

=cut

sub virtual {
    _item( virtual => 1, @_ );
}


=head2 sort_order

Declares an integer sort value for this column. By default, Jifty will sort
columns in the order they are defined.

=cut

sub sort_order {
    _item ( sort_order => (shift @_ || 0));
}


=head2 input_filters

Sets a list of input filters on the data.  Correct usage is
C<input_filters are 'Jifty::DBI::Filter::DateTime'>.  See
L<Jifty::DBI::Filter>.

=cut

sub input_filters {
    _list( input_filters => @_ );
}

=head2 output_filters

Sets a list of output filters on the data.  Correct usage is
C<output_filters are 'Jifty::DBI::Filter::DateTime'>.  See
L<Jifty::DBI::Filter>.  You usually don't need to set this, as the
output filters default to the input filters in reverse order.

=cut

sub output_filters {
    _list( output_filters => @_ );
}

=head2 filters

Sets a list of filters on the data.  These are applied when reading
B<and> writing to the database.  Correct usage is C<filters are
'Jifty::DBI::Filter::DateTime'>.  See L<Jifty::DBI::Filter>.  In
actuality, this is the exact same as L</input_filters>, since output
filters default to the input filters, reversed.

=cut

sub filters {
    _list( input_filters => @_ );
}

=head2 since

What application version this column was last changed.  Correct usage
is C<since '0.1.5'>.

=cut

sub since {
    _list( since => @_ );
}

=head2 valid_values

A list of valid values for this column. Jifty will use this to
autoconstruct a validator for you.  This list may also be used to
generate the user interface.  Correct usage is C<valid_values are
qw/foo bar baz/>.

If you want to display different values than are stored in the DB 
you can pass a list of hashrefs, each containing two keys, display 
and value.

 valid_values are
  { display => 'Blue', value => 'blue' },
  { display => 'Red', value => 'red' }

=cut

sub valid_values {
    _list( valid_values => @_ );
}

=head2 valid

Alias for C<valid_values>.

=cut

sub valid {
    _list( valid_values => @_ );
}

=head2 label

Designates a human-readable label for the column, for use in user
interfaces.  Correct usage is C<label is 'Your foo value'>.

=cut

sub label {
    _list( label => @_ );
}

=head2 hints

A sentence or two to display in long-form user interfaces about what
might go in this column.  Correct usage is C<hints is 'Used by the
frobnicator to do strange things'>.

=cut

sub hints {
    _list( hints => @_ );
}

=head2 render_as

Used in user interface generation to know how to render the column.

The values for this attribute are the same as the names of the modules under
L<Jifty::Web::Form::Field>, i.e. 

=over 

=item * Button

=item * Checkbox

=item * Combobox

=item * Date

=item * Hidden

=item * InlineButton

=item * Password

=item * Radio

=item * Select

=item * Textarea

=item * Upload

=item * Unrendered

=back

You may also use the same names with the initial character in lowercase. 

The "Unrendered" may seem counter-intuitive, but is there to allow for
internal fields that should not actually be displayed.

If these don't meet your needs, you can write your own subclass of
L<Jifty::Web::Form::Field>. See the documentation for that module.

=cut

sub render_as {
    _list( render_as => @_ );
}

=head2 render

Alias for C<render_as>.

=cut

sub render {
    _list( render_as => @_ );
}

=head2 by

Helper method to improve readability.

=cut

sub by {
    _list( by => @_ );
}

=head2 is

Helper method to improve readability.

=cut

sub is {
    my $thing = shift;
    ref $thing eq "ARRAY" ? ( @{$thing}, @_ ) : ($thing, @_);
}

=head2 as

Helper method to improve readability.

=cut

sub as {
    my $thing = shift;
    ref $thing eq "ARRAY" ? ( @{$thing}, @_ ) : ($thing, @_);
}

=head2 are

Helper method to improve readability.

=cut

sub are {
    my $ref = [];
    push @{$ref}, shift @_ while @_ and not UNIVERSAL::isa($_[0], "Jifty::DBI::Schema::Trait");
    return( $ref, @_ );
}

=head2 on

Helper method to improve readability.

=cut

sub on {
    _list( self => shift );
}

sub _list {
    defined wantarray
        or croak("Cannot add traits in void context -- check for misspelled preceding comma as a semicolon or missing use statements for models you refer_to.");

    wantarray
        or croak("Cannot call list traits in scalar context -- check for unneccessary 'is'");
    _trait(@_);
}

sub _item {
    defined wantarray
        or croak("Cannot add traits in void context -- check for misspelled preceding comma as a semicolon");

    _trait(@_);
}

sub _trait {
    my @trait;
    push @trait, shift @_ while @_ and not UNIVERSAL::isa($_[0], "Jifty::DBI::Schema::Trait");
    return wantarray ? (Jifty::DBI::Schema::Trait->new(@trait), @_) : Jifty::DBI::Schema::Trait->new(@trait);
}

=head1 EXAMPLE

=head1 AUTHOR

=head1 BUGS

=head1 SUPPORT

=head1 COPYRIGHT & LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

package Jifty::DBI::Schema::Trait;

use overload "!" => \&negation;

sub new {
    my $class = shift;
    return bless [@_], $class;
}

sub apply {
    my $self = shift;
    my ($column) = @_;

    my ($method, $argument) = @{$self};

    croak("Illegal Jifty::DBI::Schema property '$method'")
      unless $column->can($method);

    $column->$method($argument);
}

sub negation {
    my $self = shift;
    my ($trait, @rest) = @{$self};
    return (ref $self)->new($trait, map {not $_} @rest);
}

1;
