package Jifty::DBI::Record::Plugin;

use warnings;
use strict;
use Carp;

sub import {
    my $self = shift;
    my $caller = caller;
    for ($self->columns) {
            $caller->COLUMNS->{$_->name} = $_ ;
            $self->_init_methods_for_column($_);
    }
 return 1;
}

1;
