use warnings;
use strict;

package Jifty::DBI::Column;

use base qw/Class::Accessor/;

__PACKAGE__->mk_accessors qw/
    name 
    type 
    default 
    validator 
    boolean 
    readable writable 
    length 
    refers_to_collection_class
    refers_to_record_class
    alias_for_column 
    filters
    input_filters
    output_filters
    /;

=head1 NAME

Jifty::DB::Column

=head1 DESCRIPTION


This class encapsulate's a single column in a Jifty::DBI::Record table description. It replaces the _accessible method in
L<Jifty::DBI::Record>.

It has the following accessors: C<name type default validator boolean refers_to readable writable length>.

=cut


sub new {
    my $class = shift;
    my $self = {};
    bless $self => $class;
    return $self;
}

sub is_numeric {
    my $self = shift;
    if ($self->type     =~ /INT|NUMERIC|DECIMAL|REAL|DOUBLE|FLOAT/i ) {
        return 1;
    }
    return 0;

}

# Aliases for compatibility with searchbuilder code
*read = \&readable;
*write = \&writable;


sub decode_value {
    my $self = shift;
    my $value_ref = shift;
    $self->_apply_filters( value_ref => $value_ref, 
                           filters  => (reverse $self->filters, $self->output_filters),
                           action => 'decode'
                        );
}


sub encode_value {
    my $self = shift;
    my $value_ref = shift;
    $self->_apply_filters( value_ref => $value_ref, 
                           filters  => ($self->input_filters, $self->filters),
                           action => 'encode'
                        );
}


sub _apply_filters {
    my $self = shift;
    my %args = (
        value_ref => undef,
        filters   => undef,
        action    => undef,
        @_
    );
    my $action = $args{'action'};
    foreach my $filter_class ( @{ $args{filters} } ) {
        $filter_class->require();

        # XXX TODO error proof this
        $filter_class->$action( $args{value_ref} );
    }
}


1;
