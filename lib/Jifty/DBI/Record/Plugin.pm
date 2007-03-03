package Jifty::DBI::Record::Plugin;

use warnings;
use strict;


use base qw/Exporter/;


sub import {
    my $self = shift;
    my $caller = caller;
    for ($self->columns) {
            $caller->COLUMNS->{$_->name} = $_ ;
            $caller->_init_methods_for_column($_);
    }
    $self->export_to_level(1,undef);
    
    if (my $triggers =  $self->can('register_triggers') ) {
        $triggers->($caller)
    }
}



1;
