use Test::More;
eval "use Test::NoTabs 1.00";
plan skip_all => "Test::NoTabs 1.00 required for testing POD coverage" if $@;
plan skip_all => "Tab tests only run for authors" unless (-d 'inc/.author');

all_perl_files_ok('lib', 't');
