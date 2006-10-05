use inc::Module::Install;
requires( perl => '5.8.3');
name ('Jifty-DBI');
license ('perl');
version_from('lib/Jifty/DBI.pm');
requires('Cache::Simple::TimedExpiry' => '0.21');
requires('Class::Accessor::Fast' => 0);
requires('Class::Data::Inheritable');
requires('Class::ReturnValue', 0.40);
requires('Clone');
requires('DBI');
requires('DBIx::DBSchema' => '0.29');
requires('Data::Page');
requires('DateTime' => 0.34);
requires('DateTime::Format::ISO8601');
requires('DateTime::Format::Strptime');
requires('Encode'  => 2.10);
requires('Exporter::Lite');
requires('Lingua::EN::Inflect');
requires('UNIVERSAL::require');
build_requires('Test::More' => 0.52);
build_requires('DBD::SQLite');
no_index directory => 'ex';
features( 'Memcached support' => [
    -default => 1,
    Cache::Memcached => '']
);
auto_install();
&WriteAll;

