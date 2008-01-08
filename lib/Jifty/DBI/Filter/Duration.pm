package Jifty::DBI::Filter::Duration;

use warnings;
use strict;

use base qw|Jifty::DBI::Filter|;
use Time::Duration qw();
use Time::Duration::Parse qw();

=head1 NAME

Jifty::DBI::Filter::Duration - Encodes time durations

=head1 DESCRIPTION

=head2 encode

If value is defined, then encode it using
L<Time::Duration::Parse/parse_duration>, otherwise do nothing.

=cut

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return unless defined $$value_ref;

    # Convert hh:mm(::ss)? to something Time::Duration::Parse understands
    $$value_ref =~ s/\b(\d+):(\d\d):(\d\d)\b/$1h $2m $3s/g;
    $$value_ref =~ s/\b(\d+):(\d\d)\b/$1h $2m/g;

    $$value_ref = Time::Duration::Parse::parse_duration($$value_ref);

    return 1;
}

=head2 decode

If value is defined, then decode it using
L<Time::Duration/duration_exact> and L<Time::Duration/concise>,
otherwise do nothing.

=cut

sub decode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return unless defined $$value_ref;

    $$value_ref = Time::Duration::concise(Time::Duration::duration_exact($$value_ref));
}

=head1 SEE ALSO

L<Jifty::DBI::Filter>, L<Time::Duration>, L<Time::Duration::Parse>

=cut

1;
