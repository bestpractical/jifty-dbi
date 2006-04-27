package Jifty::DBI::Filter::Date;

use warnings;
use strict;

use base qw|Jifty::DBI::Filter|;
use DateTime                  ();
use DateTime::Format::ISO8601 ();
use DateTime::Format::Strptime ();


=head1 NAME

Jifty::DBI::Filter::Date - DateTime object wrapper around date columns

=head1 DESCRIPTION

This filter allow you to work with DateTime objects that represent "Dates",
store everything in the database in GMT and not hurt yourself badly
when you pull them out and put them in repeatedly
text dates.

=head2 encode

If value is a DateTime object then move it into a "floating" timezone
and expand it into ISO 8601 format C<YYYY-MM-DD>.  By storing it in 
the database as a floating timezone, it doesn't matter if the user's 
desired timezone changes between lookups

Does nothing if value is not defined or is a string.


=cut

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;
    return unless $$value_ref;

    return unless UNIVERSAL::isa( $$value_ref, 'DateTime' );
    $$value_ref->time_zone('floating');

    my $format = ($self->column->type eq "date" ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S");
    $$value_ref = $$value_ref->strftime($format);
    return 1;
}

=head2 decode

If we're loading something from a column that doesn't specify times, then
it's loaded into a floating timezone.

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

    my $str = join('T', split ' ', $$value_ref, 2);
    my $dt = DateTime::Format::ISO8601->parse_datetime($str);
    $dt->time_zone('floating');
    $dt->set_formatter(DateTime::Format::Strptime->new(pattern => '%Y-%m-%d'));
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
