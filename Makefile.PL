use inc::Module::Install;
requires( perl => '5.8.3');
name ('Jifty-DBI');
license ('perl');
version_from('lib/Jifty/DBI.pm');
requires('DBI');
requires('Encode'  => 2.10);
requires('UNIVERSAL::require');
requires('Lingua::EN::Inflect');
requires('Class::ReturnValue', 0.40);
requires('Class::Accessor' => 0);
requires('Clone');
requires('Cache::Simple::TimedExpiry' => '0.21');
requires('Data::Page');
requires('DateTime');
requires('DateTime::Format::Strptime');

build_requires('Test::More' => 0.52);
build_requires('DBD::SQLite');

features(
	'Schema generation' => [
	  -default => 1,
	  'DBIx::DBSchema' => '',
	  'Class::Accessor' => '',
	],
);
#auto_bundle_deps();
auto_install();
&WriteAll;

