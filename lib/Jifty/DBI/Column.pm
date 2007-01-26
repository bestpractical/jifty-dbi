use warnings;
use strict;

package Jifty::DBI::Column;

our $VERSION = '0.01';
use base qw/Class::Accessor::Fast Jifty::DBI::HasFilters/;
use UNIVERSAL::require;

__PACKAGE__->mk_accessors qw/
    name
    type
    default
    readable writable
    max_length
    mandatory
    virtual
    distinct
    sort_order
    refers_to by
    alias_for_column
    aliased_as
    since until

    label hints render_as
    valid_values
    indexed
    autocompleted
    _validator
    _checked_for_validate_sub
    record_class
    /;

=head1 NAME

Jifty::DBI::Column

=head1 DESCRIPTION


This class encapsulate's a single column in a Jifty::DBI::Record table
description. It replaces the _accessible method in
L<Jifty::DBI::Record>.

It has the following accessors: C<name type default validator boolean
refers_to readable writable length>.

=cut

sub is_numeric {
    my $self = shift;
    if ( $self->type =~ /INT|NUMERIC|DECIMAL|REAL|DOUBLE|FLOAT/i ) {
        return 1;
    }
    return 0;
}

sub validator {
    my $self = shift;

    if ( @_ ) {
        $self->_validator( shift );
    }
    elsif ( not $self->_checked_for_validate_sub and not $self->_validator ) {
        my $name = ( $self->aliased_as ? $self->aliased_as : $self->name );
        my $can  = $self->record_class->can( "validate_" . $name );
        
        $self->_validator( $can ) if $can;
        $self->_checked_for_validate_sub( 1 );
    }

    return $self->_validator;
}

# Aliases for compatibility with searchbuilder code
*read  = \&readable;
*write = \&writable;

sub length {
    Carp::carp('$column->length is deprecated; use $column->max_length instead');
    my $self = shift;
    $self->max_length(@_);
}

1;
