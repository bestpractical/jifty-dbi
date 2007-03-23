
use strict;
use warnings;

package Jifty::DBI::Filter::Truncate;
use base qw/Jifty::DBI::Filter/;
use Encode ();

=head1 NAME

Jifty::DBI::Filter::Truncate - Truncates column values

=head1 DESCRIPTION

This filter truncates column values to the correct length for their
type or to a defined max_length (for non-numeric columns).

=head2 encode

If the column is a non-numeric type and has a max_length defined,
encode will truncate to that length.  If the column is of a
type limited by definition (e.g. C<char(13)>), encode will truncate
the value to fit.

=head1 SEE ALSO

L<Jifty::DBI::Filter>

=cut

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return undef unless ( defined($$value_ref) );

    my $column = $self->column();

    my $truncate_to;
    if ( $column->max_length && !$column->is_numeric ) {
        $truncate_to = $column->max_length;
    } elsif ( $column->type && $column->type =~ /char\((\d+)\)/ ) {
        $truncate_to = $1;
    }

    return unless ($truncate_to);    # don't need to truncate

    my $utf8 = Encode::is_utf8($$value_ref);
    {
        use bytes;
        $$value_ref = substr( $$value_ref, 0, $truncate_to );
    }
    if ($utf8) {

        # return utf8 flag back, but use Encode::FB_QUIET because
        # we could broke tail char
        $$value_ref = Encode::decode_utf8( $$value_ref, Encode::FB_QUIET );
    }
}

1;
