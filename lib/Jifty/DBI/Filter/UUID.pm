package Jifty::DBI::Filter::UUID;

use warnings;
use strict;

use base qw|Jifty::DBI::Filter|;
use Data::UUID;

our $UUID_GEN = Data::UUID->new();



=head1 NAME

Jifty::DBI::Filter::uuid - Sets column to a UUID

=head1 DESCRIPTION

UUID columns

=head2 encode

If value is not efined, sets it to a new UUID. Otherwise does nothing

=cut

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return unless $$value_ref;

    $$value_ref = $UUID_GEN->create_str();


    return 1;
}


=head1 SEE ALSO

L<Jifty::DBI::Filter>, L<MIME::Base64>

=cut

1;
