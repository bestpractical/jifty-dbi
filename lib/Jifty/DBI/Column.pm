use warnings;
use strict;

package Jifty::DBI::Column;

use base qw/Class::Accessor/;
use UNIVERSAL::require;


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
    output_filters
    /;

#    input_filters

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
                           filters  => $self->output_filters,
                           action => 'decode'
                        );
}

sub input_filters {
    my $self = shift;

    return (['Jifty::DBI::Filter::Truncate']);

}



sub encode_value {
    my $self = shift;
    my $value_ref = shift;
    $self->_apply_filters( value_ref => $value_ref, 
                           filters  => 
                           $self->input_filters,
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
        my $filter = $filter_class->new( column => $self, value_ref => $args{'value_ref'});
        # XXX TODO error proof this
        $filter->$action();
    }
}


1;
