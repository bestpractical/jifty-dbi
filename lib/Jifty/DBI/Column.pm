use warnings;
use strict;

package Jifty::DBI::Column;

our $VERSION = '0.01';
use base qw/Class::Accessor::Fast Jifty::DBI::HasFilters/;
use UNIVERSAL::require;
use version;

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
    since till

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

sub length { Carp::croak('$column->length is no longer supported; use $column->max_length instead') }
sub until { Carp::croak('$column->until is no longer supported; use $column->till instead') }

=head2 active

Returns the a true value if the column method exists for the current application
version. The current application version is determined by checking the L<Jifty::DBI::Record/schema_version> of the column's L</record_class>. This method returns a false value if the column is not yet been added or has been dropped.

This method returns a false value under these circumstances:

=over

=item *

Both the C<since> trait and C<schema_version> method are defined and C<schema_version> is less than the version set on C<since>.

=item *

Both the C<till> trait and C<schema_version> method are defined and C<schema_version> is greater than or equal to the version set on C<till>.

=back

Otherwise, this method returns true.

=cut

sub active {
    my $self    = shift;

    return 1 unless $self->record_class->can('schema_version');
    return 1 unless defined $self->record_class->schema_version;

    my $version = version->new($self->record_class->schema_version);

    # The application hasn't yet started using this column
    return 0 if defined $self->since
            and $version < version->new($self->since);

    # The application stopped using this column
    return 0 if defined $self->till
            and $version >= version->new($self->till);

    # The application currently uses this column
    return 1;
}

=head2 active

Returns the a true value if the column method exists for the current application
version. The current application version is determined by checking the L<Jifty::DBI::Record/schema_version> of the column's L</record_class>. This method returns a false value if the column is not yet been added or has been dropped.

This method returns a false value under these circumstances:

=over

=item *

Both the C<since> trait and C<schema_version> method are defined and C<schema_version> is less than the version set on C<since>.

=item *

Both the C<till> trait and C<schema_version> method are defined and C<schema_version> is greater than or equal to the version set on C<till>.

=back

Otherwise, this method returns true.

=cut

sub active {
    my $self    = shift;

    return 1 unless $self->record_class->can('schema_version');
    return 1 unless defined $self->record_class->schema_version;

    my $version = version->new($self->record_class->schema_version);

    # The application hasn't yet started using this column
    return 0 if defined $self->since
            and $version < version->new($self->since);

    # The application stopped using this column
    return 0 if defined $self->till
            and $version >= version->new($self->till);

    # The application currently uses this column
    return 1;
}

1;
