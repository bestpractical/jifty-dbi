package Jifty::DBI::Schema;
use Carp qw/carp/;
use Exporter::Lite;
our @EXPORT
    = qw(column type default validator immutable unreadable length distinct mandatory not_null valid_values label hints render_as since input_filters output_filters is by are on);

our $SCHEMA;

sub column {
    my $name = shift;

    my $from = (caller)[0];
    $from =~ s/::Schema//;
    $from->_init_columns;

    my @args = (
        name     => $name,
        readable => 1,
        writable => 1,
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
        $column->type('integer');

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
        }
        elsif ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Collection' ) ) {
            $column->by('id') unless $column->by;
        }
        else {
            warn "Error: $refclass neither Record nor Collection";
        }
    }

    $from->COLUMNS->{$name} = $column;
}

sub type ($) {
    return ( type => shift );
}

sub default ($) {
    return ( default => shift );
}

sub validator ($) {
    return ( validator => shift );
}

sub immutable () {
    return ( [ writable => 0 ] );
}

sub unreadable {
    return ( [ readable => 0 ] );
}

sub length ($) {
    return ( length => shift );
}

sub mandatory () {
    return ( [ mandatory => 1 ] );
}

sub distinct () {
    return ( [ distinct => 1 ] );
}

sub not_null () {
    carp "'is not_null' is deprecated in favor of 'is mandatory'";
    return ( [ mandatory => 1 ] );
}

sub input_filters ($) {
    return ( input_filters => shift );
}

sub output_filters ($) {
    return ( output_filters => shift );
}

sub since ($) {
    return ( since => shift );
}

sub valid_values ($) {
    return ( valid_values => shift );
}

sub label ($) {
    return ( label => shift );
}

sub hints ($) {
    return ( hints => shift );
}

sub render_as ($) {
    return ( render_as => shift );
}

sub is ($) {
    my $thing = shift;
    return ref $thing eq "ARRAY" ? @{$thing} : $thing;
}

sub by ($) {
    return ( by => shift );
}

sub are (@) {
    return [@_];
}

sub on ($) {
    return ( self => shift );
}

1;
