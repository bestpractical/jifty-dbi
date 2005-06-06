use inc::Module::Install;

name ('DBIx-SearchBuilder');
license ('perl');
version_from('SearchBuilder.pm');
requires('DBI');
requires('Want');
requires('Encode');
requires('Class::ReturnValue', 0.40);
requires('Cache::Simple::TimedExpiry' => '0.21');
build_requires('Test::More' => 0.52);

features(
	'Lower case API' => [
		-default => 0,
		'capitalization' => '0.03',
	],
	'Schema generation' => [
	  -default => 1,
	  'DBIx::DBSchema' => '',
	  'Class::Accessor' => '',
	],
);
auto_install();

&Makefile->write;
&Meta->write;
