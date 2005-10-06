use inc::Module::Install;

name ('Jifty-DBI');
license ('perl');
version_from('lib/Jifty/DBI.pm');
requires('DBI');
requires('Want');
requires('Encode');
requires('UNIVERSAL::require');
requires('Lingua::EN::Inflect');
requires('Class::ReturnValue', 0.40);
requires('Cache::Simple::TimedExpiry' => '0.21');
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
auto_install();

&WriteAll;

