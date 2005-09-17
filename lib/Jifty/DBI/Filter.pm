use warnings;
use strict;

package Jifty::DBI::Filter;


sub new {
    my $class = shift;
    my $self = {};
    bless $self, $class;
    return ($self);

}

=head2 encode

C<encode> takes data that users are handing to us and marshals it into
a form suitable for sticking it in the database. This could be anything
from flattening a L<DateTime> object into an ISO date to making sure
that data is utf8 clean.


C<encode> takes two named parameters:

=over

=item value_ref

A reference to the current value you're going to be massaging. C<encode> works in place, massaging whatever value_ref refers to.

=item column

A L<Jifty::DBI::Column> object, whatever sort of column we're working with here.

=back



=cut


sub encode {
    my $self = shift;
    my %args = ( value_ref => undef,
                 column => undef,
                 @_);

}

=head2 decode

C<decode> takes data that the database is handing back to us and gets it into a form that's OK to hand back to the user. This could be anything
from flattening a L<DateTime> object into an ISO date to making sure
that data is utf8 clean.


C<decode> takes two named parameters:

=over

=item value_ref

A reference to the current value you're going to be massaging. C<decode> works in place, massaging whatever value_ref refers to.

=item column

A L<Jifty::DBI::Column> object, whatever sort of column we're working with here.

=back



=cut

sub decode {
    my $self = shfit;
    my %args = ( value_ref => undef,
                 column => undef,
                 @_);

}


1;
