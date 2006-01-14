use warnings;
use strict;

package Jifty::DBI::Schema;

use Carp qw/carp/;
use Exporter::Lite;
our @EXPORT
    = qw(column type default validator immutable unreadable length distinct mandatory not_null valid_values label hints render_as since input_filters output_filters is by are on virtual);

our $SCHEMA;

sub column {
    my $name = lc(shift);

    my $from = (caller)[0];
    $from =~ s/::Schema//;
    $from->_init_columns;

    my @args = (
        name     => $name,
        readable => 1,
        writable => 1,
        virtual  => 0,
        type     => 'varchar(255)',
        @_,
    );
    my @original = @args;

    my $column = Jifty::DBI::Column->new();
    while (@args) {
        my ( $method, $arguments ) = splice @args, 0, 2;
        $column->$method($arguments);
    }

    if ( my $refclass = $column->refers_to ) {
        $refclass->require();
        $column->type('integer') unless ( $column->type );

        if ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Record' ) ) {
            if ( $name =~ /(.*)_id$/ ) {
                my $virtual_column = $from->add_column($1);
                while (@original) {
                    my ( $method, $arguments ) = splice @original, 0, 2;
                    $virtual_column->$method($arguments);
                }
                $column->refers_to(undef);
                $virtual_column->alias_for_column($name);
            }
            $column->by('id') unless $column->by;
        } elsif ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Collection' ) ) {
            $column->by('id') unless $column->by;
            $column->virtual('1');
        } else {
            warn "Error: $refclass neither Record nor Collection";
        }
    }

    $from->COLUMNS->{$name} = $column;
}

sub type ($) {
    _list( type => shift );
}

sub default ($) {
    _list( default => shift );
}

sub validator ($) {
    _list( validator => shift );
}

sub immutable () {
    _item( [ writable => 0 ] );
}

sub unreadable {
    _item( [ readable => 0 ] );
}

sub length ($) {
    _list( length => shift );
}

sub mandatory () {
    _item( [ mandatory => 1 ] );
}

sub distinct () {
    _item( [ distinct => 1 ] );
}

sub not_null () {
    carp "'is not_null' is deprecated in favor of 'is mandatory'";
    _item( [ mandatory => 1 ] );
}

sub input_filters ($) {
    _list( input_filters => shift );
}

sub output_filters ($) {
    _list( output_filters => shift );
}

sub since ($) {
    _list( since => shift );
}

sub valid_values ($) {
    _list( valid_values => shift );
}

sub label ($) {
    _list( label => shift );
}

sub hints ($) {
    _list( hints => shift );
}

sub render_as ($) {
    _list( render_as => shift );
}

sub is ($) {
    my $thing = shift;
    ref $thing eq "ARRAY" ? _list( @{$thing} ) : _item($thing);
}

sub by ($) {
    _list( by => shift );
}

sub are (@) {
    _item( [@_] );
}

sub on ($) {
    _list( self => shift );
}

sub _list {
    defined wantarray
        or die
        "Cannot add traits in void context -- check for misspelled preceding comma as a semicolon";
    wantarray
        or die
        "Cannot call list traits in scalar context -- check for unneccessary 'is'";
    @_;
}

sub _item {
    defined wantarray
        or die
        "Cannot add traits in void context -- check for misspelled preceding comma as a semicolon";
    $_[0];
}

1;
