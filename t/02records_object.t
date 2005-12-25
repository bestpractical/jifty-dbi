#!/usr/bin/perl -w


use strict;
use warnings;
use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@AvailableDrivers);

use constant TESTS_PER_DRIVER => 11;

my $total = scalar(@AvailableDrivers) * TESTS_PER_DRIVER;
plan tests => $total;

foreach my $d ( @AvailableDrivers ) {
SKIP: {
	unless( has_schema( 'TestApp', $d ) ) {
		skip "No schema for '$d' driver", TESTS_PER_DRIVER;
	}
	unless( should_test( $d ) ) {
		skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
	}

	my $handle = get_handle( $d );
	connect_handle( $handle );
	isa_ok($handle->dbh, 'DBI::db');

	my $ret = init_schema( 'TestApp', $handle );
	isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back");

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
	isa_ok( $obj, 'TestApp::Employee');
	is($obj->id, $e_id);
	is($obj->Name, 'RUZ');

	# tests for no object mapping
	my ($state, $msg) = $phone->ValueObj($handle);
	ok(!$state, "State is false");
	is( $msg, 'No object mapping for field', 'Error message is correct');

	cleanup_schema( 'TestApp', $handle );
}} # SKIP, foreach blocks

1;


package TestApp;
sub schema_sqlite {
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

sub schema_mysql {
[ q{
CREATE TEMPORARY TABLE Employees (
	id integer AUTO_INCREMENT primary key,
	Name varchar(36)
)
}, q{
CREATE TEMPORARY TABLE Phones (
	id integer AUTO_INCREMENT primary key,
	Employee integer NOT NULL,
	Phone varchar(18)
)
} ]
}

sub schema_pg {
[ q{
CREATE TEMPORARY TABLE Employees (
	id serial PRIMARY KEY,
	Name varchar
)
}, q{
CREATE TEMPORARY TABLE Phones (
	id serial PRIMARY KEY,
	Employee integer references Employees(id),
	Phone varchar
)
} ]
}

package TestApp::Employee;

use base qw/Jifty::DBI::Record/;
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

use base qw/Jifty::DBI::Record/;

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
