
=head1 NAME

DBIx::SearchBuilder::Handle::mysqlPP

=head1 DESCRIPTION

A handler for the "pure perl" mysql database handler

=cut

package DBIx::SearchBuilder::Handle::mysqlPP;                                  
use DBIx::SearchBuilder::Handle::mysql;                                        
@ISA = qw(DBIx::SearchBuilder::Handle::mysql);                                 
                                                                               
use vars qw($VERSION @ISA $DBIHandle $DEBUG);                                  
use strict;                                                                    
