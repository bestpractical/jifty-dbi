use inc::Module::Install;

name ('DBIx-SearchBuilder');
version_from('SearchBuilder.pm');
requires('DBI');
requires('Class::ReturnValue', 0.40);
build_requires('Test::More');

&Makefile->write;
&Meta->write;
