
use strict;
use warnings;

package Jifty::DBI::Filter::Truncate;
use base qw/Jifty::DBI::Filter/;
use Encode ();

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return undef unless ( defined($$value_ref) );

    my $column = $self->column();

    my $truncate_to;
    if ( $column->length && !$column->is_numeric ) {
        $truncate_to = $column->length;
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
