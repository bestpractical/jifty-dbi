#!/usr/bin/perl -w


use strict;
use warnings;
use File::Spec;

BEGIN { require "t/utils.pl" }

use Test::More;
eval "use DBD::SQLite";
if ($@) { 
plan skip_all => "DBD::SQLite required for testing database interaction" 
} else{
plan tests => 9;
}
my $handle = get_handle('SQLite');
$handle->Connect( Driver => 'SQLite', Database => File::Spec->catfile(File::Spec->tmpdir(), "sb-test.$$"));
isa_ok($handle->dbh, 'DBI::db');

foreach( @{ TestApp->schema } ) {
	my $ret = $handle->SimpleQuery($_);
	isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back");
}


my $emp = TestApp::Employee->new($handle);
my $e_id = $emp->Create( Name => 'RUZ' );
ok($e_id, "Got an ide for the new emplyee");
my $phone = TestApp::Phone->new($handle);
isa_ok( $phone, 'TestApp::Phone', "it's atestapp::phone");
my $p_id = $phone->Create( Employee => $e_id, Phone => '+7(903)264-03-51');
# XXX: test fails if next string is commented
is($p_id, 1, "Loaded record $p_id");
$phone->Load( $p_id );

my $obj = $phone->EmployeeObj($handle);
ok($obj, "Employee #$e_id has phone #$p_id");
is($obj->id, $e_id);
is($obj->Name, 'RUZ');


package TestApp;
sub schema {
[
q{
CREATE TABLE Employees (
	id integer primary key,
	Name varchar(36)
)
}, q{
CREATE TABLE Phones (
	id integer primary key,
	Employee integer NOT NULL,
	Phone varchar(18)
) }
]

}

package TestApp::Employee;

use base qw/DBIx::SearchBuilder::Record/;
use vars qw/$VERSION/;
$VERSION=0.01;

sub _Init {
    my $self = shift;
    my $handle = shift;
    $self->Table('Employees');
    $self->_Handle($handle);
}

sub _ClassAccessible {
    {   
        
        id =>
        {read => 1, type => 'int(11)'}, 
        Name => 
        {read => 1, write => 1, type => 'varchar(18)'},

    }
}

1;

package TestApp::Phone;

use vars qw/$VERSION/;
$VERSION=0.01;

use base qw/DBIx::SearchBuilder::Record/;

sub _Init {
    my $self = shift;
    my $handle = shift;
    $self->Table('Phones');
    $self->_Handle($handle);
}

sub _ClassAccessible {
    {   
        
        id =>
        {read => 1, type => 'int(11)'}, 
        Employee => 
        {read => 1, write => 1, type => 'int(11)', object => 'TestApp::Employee' },
        Value => 
        {read => 1, write => 1, type => 'varchar(18)'},

    }
}


1;
