package DBIx::SearchBuilder::Handle::mysqlPP;                                  
use DBIx::SearchBuilder::Handle::mysql;                                        
@ISA = qw(DBIx::SearchBuilder::Handle::mysql);                                 
                                                                               
use vars qw($VERSION @ISA $DBIHandle $DEBUG);                                  
use strict;                                                                    

1;

__END__

=head1 NAME

DBIx::SearchBuilder::Handle::mysqlPP - A mysql specific Handle object

=head1 DESCRIPTION

A Handle subclass for the "pure perl" mysql database driver.

This is currently identical to the DBIx::SearchBuilder::Handle::mysql class.

=head1 AUTHOR



=head1 SEE ALSO

DBIx::SearchBuilder::Handle::mysql

=cut

