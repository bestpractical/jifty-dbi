use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'	=> 'DBIx::SearchBuilder',
    'VERSION_FROM' => 'SearchBuilder.pm', # finds $VERSION
    'PREREQ_PM' => { 'Class::ReturnValue' => '0.40'},
);
