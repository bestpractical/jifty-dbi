package Jifty::DBI::HasFilters;

use warnings;
use strict;

use base qw/Class::Accessor::Fast/;
__PACKAGE__->mk_accessors qw/
    input_filters
    output_filters
    filters
    /;

=head1 NAME

Jifty::DBI::HasFilters - abstract class for objects that has filters

=head1 SYNOPSYS

  my $record = Jifty::DBI::Record->new(...);
  $record->input_filters( 'Jifty::DBI::Filter::Truncate',
                          'Jifty::DBI::Filter::utf8'
                        );
  my @filters = $record->output_filters;

=head1 DESCRIPTION

This abstract class provide generic interface for setting and getting
input and output data filters for L<Jifty::DBI> objects.
You shouldn't use it directly, but L<Jifty::DBI::Handle>, L<Jifty::DBI::Record>
and L<Jifty::DBI::Column> classes inherit this interface.


=head1 METHODS

=head2 input_filters

Returns array of the input filters, if arguments list is not empty
then set input filter.

=cut

sub input_filters {
    my $self = shift;
    if (@_) {    # setting
        my @values = map { UNIVERSAL::isa( $_, 'ARRAY' ) ? @$_ : $_ } @_;
        $self->_input_filters_accessor( [@values] );
    }

    return @{ $self->_input_filters_accessor || [] };
}

=head2 output_filters

Deals similar with list of output filters, but unless
you defined own list returns reversed list of the input
filters. In common situation you don't need to define
own list of output filters, but use this method to get
default list based on the input list.

=cut

sub output_filters {
    my $self = shift;
    if (@_) {    # setting
        my @values = map { UNIVERSAL::isa( $_, 'ARRAY' ) ? @$_ : $_ } @_;
        $self->_output_filters_accessor( [@values] );
    }

    my @values = @{ $self->_output_filters_accessor || [] };
    return @values if @values;

    @values = reverse $self->input_filters;
    return @values;
}

=head2 filters FILTERS

Sets the input and output filters at the same time.  Returns a hash,
with keys C<input> and C<output>, whose values are array references to
the respective lists.

=cut

sub filters {
    my $self = shift;
    return {
        output => $self->output_filters(@_),
        input  => $self->input_filters(@_)
    };
}

=head1 SEE ALSO

L<Jifty::DBI::Filter>

=cut

1;
