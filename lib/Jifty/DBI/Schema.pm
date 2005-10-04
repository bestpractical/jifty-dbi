package Jifty::DBI::Schema;
use Exporter::Lite;
our @EXPORT = qw(column type default validator immutable length not_null valid_values input_filters output_filters mandatory is by are on);

our $SCHEMA;
sub column {
    my $name = shift;

    my $from = (caller)[0];
    $from =~ s/::Schema//;
    $from->_init_columns;

    my @args = (name     => $name,
                readable => 1,
                writable => 1,
                null     => 1,
                @_,
               );

    my $column = Jifty::DBI::Column->new();
    while (@args) {
        my ($method, $arguments) = splice @args, 0, 2;
        $column->$method($arguments);
    }

    if (my $refclass = $column->refers_to) {
        $refclass->require();
        $column->type('integer');

        if ( UNIVERSAL::isa( $refclass, 'Jifty::DBI::Record' ) ) {
            if ( $name =~ /(.*)_id$/ ) {
                my $virtual_column = $from->add_column($1);
                $virtual_column->refers_to($refclass);
                $virtual_column->alias_for_column($name);
            }
            else {
                $column->refers_to($refclass);
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
    return (type => shift);
}

sub default ($) {
    return (default => shift);
}

sub validator ($) {
    return (validator => shift);
}

sub immutable () {
    return ([writable => 0]);
}

sub length ($) {
    return (length => shift);
}

sub not_null () {
    return ([null => 0]);
}

sub input_filters ($) {
    return (input_filters => shift);
}

sub output_filters ($) {
    return (output_filters => shift);
}



sub mandatory () {
    return ([mandatory => 1]);
}

sub valid_values ($) {
    return (valid_values => shift);
}



sub is ($) {
    my $thing = shift;
    return ref $thing ? @{$thing} : $thing;
}

sub by ($) {
    return (by => shift);
}

sub are (@) {
    return [@_];
}

sub on ($) {
    return (self => shift);
}

1;
