use inc::Module::Install;
requires( perl => '5.8.3');
name ('Jifty-DBI');
license ('perl');
version_from('lib/Jifty/DBI.pm');
requires('Cache::Simple::TimedExpiry' => '0.21');
requires('Class::Accessor::Fast' => 0);
requires('Class::Data::Inheritable');
requires('Class::ReturnValue', 0.40);
requires('Class::Trigger', 0.12);
requires('Clone');
requires('DBI');
requires('DBIx::DBSchema' => '0.34');
requires('Data::Page');
requires('DateTime' => 0.34);
requires('DateTime::Format::ISO8601');
requires('DateTime::Format::Strptime');
requires('Encode'  => 2.10);
requires('Exporter::Lite');
requires('Hash::Merge');
requires('Lingua::EN::Inflect');
requires('Object::Declare' => 0.22);
requires('UNIVERSAL::require' => 0.11);
requires('Scalar::Defer' => 0.10);
requires('version');
#requires('Class::Trigger');
build_requires('Test::More' => 0.52);
build_requires('Test::Warn' => 0.10);
build_requires('DBD::SQLite');
no_index directory => 'ex';
features( 
    'Memcached support' => [
        -default => 1,
        'Cache::Memcached' => ''
    ],
    'YAML filter' => [
        -default => 1,
        (can_cc() ? requires('YAML::Syck') : requires('YAML')),
    ],
    'Duration filter' => [
        -default => 1,
        'Time::Duration' => '',
        'Time::Duration::Parse' => '0.05',
    ],
    'URI filter' => [
        -default => 1,
        'URI' => '',
    ],
);
auto_install();
&WriteAll;

