use warnings;
use strict;

package Jifty::DBI::Column;

our $VERSION = '0.01';
use base qw/Class::Accessor Jifty::DBI::HasFilters/;
use UNIVERSAL::require;

__PACKAGE__->mk_accessors qw/
    name
    type
    default
    validator
    readable writable
    length
    mandatory
    virtual
    distinct
    sort_order
    refers_to by
    alias_for_column
    since until

    label hints render_as
    valid_values
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

# Aliases for compatibility with searchbuilder code
*read  = \&readable;
*write = \&writable;

1;
