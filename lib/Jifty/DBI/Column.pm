use warnings;
use strict;

package Jifty::DBI::Column;

use base qw/Class::Accessor/;

__PACKAGE__->mk_accessors qw/name type default validator 
    boolean 
    readable writable 
    length 
    refers_to_collection_class
    refers_to_record_class
    alias_for_column 
    /;

=head1 NAME

Jifty::DB::Column

=head1 DESCRIPTION


This class encapsulate's a single column in a Jifty::DBI::Record table description. It replaces the _accessible method in
L<Jifty::DBI::Record>.

It has the following accessors: C<name type default validator boolean refers_to readable writable length>.

=cut


sub new {
    my $class = shift;
    my $self = {};
    bless $self => $class;
    return $self;
}

sub is_numeric {
    my $self = shift;
    if ($self->type     =~ /INT|NUMERIC|DECIMAL|REAL|DOUBLE|FLOAT/i ) {
        return 1;
    }
    return 0;

}
1;
