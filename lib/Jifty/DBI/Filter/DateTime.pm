package Jifty::DBI::Filter::DateTime;

use warnings;
use strict;

use base qw|Jifty::DBI::Filter|;
use DateTime                  ();
use DateTime::Format::ISO8601 ();
use DateTime::Format::Strptime ();
use Carp ();

use constant _time_zone => '';
use constant _strptime  => '%Y-%m-%d %H:%M:%S';


=head1 NAME

Jifty::DBI::Filter::DateTime - DateTime object wrapper around date columns

=head1 DESCRIPTION

This filter allow you to work with DateTime objects instead of
plain text dates.  If the column type is "date", then the hour,
minute, and second information is discarded when encoding.

=head2 encode

If value is DateTime object then converts it into ISO format
C<YYYY-MM-DD hh:mm:ss>. Does nothing if value is not defined.

Sets the value to undef if the value is a string and doesn't match an ISO date (at least).


=cut

sub encode {
    my $self = shift;

    my $value_ref = $self->value_ref;

    return if !defined $$value_ref;

    if  ( ! UNIVERSAL::isa( $$value_ref, 'DateTime' )) {
        if ( $$value_ref !~ /^\d{4}[ -]?\d{2}[ -]?[\d{2}]/) {
       $$value_ref = undef;
        }
        return undef;
   }

    return unless $$value_ref;
    if (my $tz = $self->_time_zone) {
	$$value_ref = $$value_ref->clone;
	$$value_ref->time_zone('floating');
    }
    $$value_ref = $$value_ref->strftime($self->_strptime);
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

    my $str = join('T', split ' ', $$value_ref, 2);
    my $dt;
    eval { $dt  = DateTime::Format::ISO8601->parse_datetime($str) };

    if ($@) { # if datetime can't decode this, scream loudly with a useful error message
        Carp::cluck($@);
        return;
    }

    if ($dt) {
	my $tz = $self->_time_zone;
	$dt->time_zone($tz) if $tz;

        $dt->set_formatter(DateTime::Format::Strptime->new(pattern => $self->_strptime));
        $$value_ref = $dt;
    } else {
        return;
    }
}

=head1 SEE ALSO

L<Jifty::DBI::Filter>, L<DateTime>

=cut

1;
