package Jifty::DBI::Filter::DateTime;

use warnings;
use strict;

use base qw|Jifty::DBI::Filter|;
use DateTime                  ();
use DateTime::Format::ISO8601 ();

=head1 NAME

Jifty::DBI::Filter::DateTime - DateTime object wrapper around date columns

=head1 DESCRIPTION

This filter allow you to work with DateTime objects instead of plain
text dates.

=head2 encode

If value is DateTime object then converts it into ISO format
C<YYYY-MM-DD hh:mm:ss>. Does nothing if value is not defined or
string.

=cut

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return unless $$value_ref;

    return unless UNIVERSAL::isa( $$value_ref, 'DateTime' );

    $$value_ref = $$value_ref->strftime("%Y-%m-%d %H:%M:%S");

    return 1;
}

=head2 decode

If value is defined then converts it into DateTime object otherwise do
nothing.

=cut

sub decode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return unless defined $$value_ref;

# XXX: Looks like we should use special modules for parsing DT because
# different MySQL versions can return DT in different formats(none strict ISO)
# Pg has also special format that depends on "european" and
#    server time_zone, by default ISO
# other DBs may have own formats(Interbase for example can be forced to use special format)
# but we need Jifty::DBI::Handle here to get DB type

    my $str = $$value_ref;
    $str =~ s/ /T/;

    my $dt = DateTime::Format::ISO8601->parse_datetime($str);
    if ($dt) {
        $$value_ref = $dt;
    } else {
        return;
    }
}

=head1 SEE ALSO

L<Jifty::DBI::Filter>, L<DateTime>

=cut

1;
