use inc::Module::Install;

name ('DBIx-SearchBuilder');
license ('perl');
version_from('SearchBuilder.pm');
requires('DBI');
requires('Want');
requires('Class::ReturnValue', 0.40);
requires( 'Cache::Simple::TimedExpiry' => '0.01');
build_requires('Test::More');

&Makefile->write;
&Meta->write;
