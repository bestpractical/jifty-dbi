package Jifty::DBI::Filter::Boolean;

use warnings;
use strict;

use base 'Jifty::DBI::Filter';

use constant TRUE_VALUES  => qw(1 t true y yes TRUE);
use constant FALSE_VALUES => qw(0 f false n no FALSE);

sub _is_true {
    my $self = shift;
    my $value = shift;

    for ($self->TRUE_VALUES) {
        return 1 if $value eq $_;
    }

    return 0;
}

sub _is_false {
    my $self = shift;
    my $value = shift;

    for ($self->FALSE_VALUES) {
        return 1 if $value eq $_;
    }

    return 0;
}

=head1 NAME

Jifty::DBI::Filter::Boolean - Encodes booleans

=head1 DESCRIPTION

=head2 encode

Transform the value into 1 or 0 so Perl's concept of the boolean's value agrees
with the database's concept of the boolean's value. (For example, 't' and 'f'
might be used -- 'f' is true in Perl)

If the value is C<undef>, then the encoded value will also be C<undef>.

=cut

sub encode {
    my $self = shift;
    my $value_ref = $self->value_ref;

    return unless defined $$value_ref;

    $$value_ref = $self->_is_true($$value_ref);
}

=head2 decode

Transform the value to the canonical true or false value as expected by the
database.

If the value is C<undef>, then the decoded value will also be C<undef>.

=cut

sub decode {
    my $self = shift;
    my $value_ref = $self->value_ref;

    return unless defined $$value_ref;

    if ($self->_is_true($$value_ref)) {
        $$value_ref = $self->handle->canonical_true;
    }
    else {
        $$value_ref = $self->handle->canonical_false;
    }
}

=head1 SEE ALSO

L<Jifty::DBI::Filter>, L<Time::Duration>, L<Time::Duration::Parse>

=cut

1;

