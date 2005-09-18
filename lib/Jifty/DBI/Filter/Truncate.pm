
use strict;
use warnings;

package Jifty::DBI::Filter::Truncate;
use base qw/Jifty::DBI::Filter/;

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return undef unless ( defined( $$value_ref ) );

    my $column = $self->column();

    my $truncate_to;
    if ( $column->length && !$column->is_numeric ) {
        $truncate_to = $column->length;
    }
    elsif ( $column->type && $column->type =~ /char\((\d+)\)/ ) {
        $truncate_to = $1;
    }

    return unless ($truncate_to);    # don't need to truncate

    # Perl 5.6 didn't speak unicode
    $$value_ref = substr( $$value_ref, 0, $truncate_to )
        unless ( $] >= 5.007 );

    require Encode;

    if ( Encode::is_utf8( $$value_ref ) ) {
        $$value_ref = Encode::decode(
            utf8 => substr(
                Encode::encode( utf8 => $$value_ref ),
                0, $truncate_to
            ),
            Encode::FB_QUIET(),
        );
    }
    else {
        $$value_ref = Encode::encode(
            utf8 => Encode::decode(
                utf8 => substr( $$value_ref, 0, $truncate_to ),
                Encode::FB_QUIET(),
            )
        );

    }

}

1;
