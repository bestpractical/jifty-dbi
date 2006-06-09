use warnings;
use strict;

package Jifty::DBI::Filter;
use base 'Class::Accessor::Fast';

__PACKAGE__->mk_accessors(qw(record column value_ref));

=head2 new

Takes

=over

=item value_ref

A reference to the current value you're going to be
massaging. C<encode> works in place, massaging whatever value_ref
refers to.

=item column

A L<Jifty::DBI::Column> object, whatever sort of column we're working
with here.

=back


=cut

sub new {
    my $class = shift;
    my %args  = (
        column    => undef,
        value_ref => undef,
        @_
    );
    my $self = {};
    bless $self, $class;

    for ( keys %args ) {
        if ( $self->can($_) ) {
            $self->$_( $args{$_} );
        }

    }

    return ($self);

}

=head2 encode

C<encode> takes data that users are handing to us and marshals it into
a form suitable for sticking it in the database. This could be anything
from flattening a L<DateTime> object into an ISO date to making sure
that data is utf8 clean.

=cut

sub encode {

}

=head2 decode

C<decode> takes data that the database is handing back to us and gets
it into a form that's OK to hand back to the user. This could be
anything from flattening a L<DateTime> object into an ISO date to
making sure that data is utf8 clean.

=cut

sub decode {

}

1;
