#!/usr/bin/perl -w


use strict;
use warnings;

use Test::More;
eval "use DBD::SQLite";
if ($@) { 
plan skip_all => "DBD::SQLite required for testing database interaction" 
} else{
plan tests => 34;
}
    my $handle;
use_ok('DBIx::SearchBuilder::Handle::SQLite');

        $handle = DBIx::SearchBuilder::Handle::SQLite->new();

isa_ok($handle, 'DBIx::SearchBuilder::Handle');
isa_ok($handle, 'DBIx::SearchBuilder::Handle::SQLite');
        $handle->Connect( Driver => 'SQLite', Database => "/tmp/sb-test.$$" );

can_ok($handle, 'dbh');
isa_ok($handle->dbh, 'DBI::db');

my $ret = $handle->SimpleQuery(TestApp::Address->schema);
isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back");


my $rec = TestApp::Address->new($handle);
isa_ok($rec, 'DBIx::SearchBuilder::Record');

# _Accessible testings
is( $rec->_Accessible('id' => 'read'), 1, 'id is accessible for read' );
is( $rec->_Accessible('id' => 'write'), undef, 'id is not accessible for write' );
is( $rec->_Accessible('unexpected_field' => 'read'), undef, "field doesn't exist and can't be accessible for read" );

can_ok($rec,'Create');

my ($id) = $rec->Create( Name => 'Jesse', Phone => '617 124 567');
ok($id,"Created record ". $id);
ok($rec->Load($id), "Loaded the record");


is($rec->id, $id, "The record has its id");
is ($rec->Name, 'Jesse', "The record's name is Jesse");

my ($val, $msg) = $rec->SetName('Obra');
ok($val, $msg) ;
is($rec->Name, 'Obra', "We did actually change the name");

# Validate immutability of the field id
($val, $msg) = $rec->Setid( $rec->id + 1 );
ok(!$val, $msg);
is($msg, 'Immutable field', 'id is immutable field');
is($rec->id, $id, "The record still has its id");

# Check some non existant field
ok( !eval{ $rec->SomeUnexpectedField }, "The record has no 'SomeUnexpectedField'");
{
	# test produce DBI warning
	local $SIG{__WARN__} = sub {return};
	is( $rec->_Value( 'SomeUnexpectedField' ), undef, "The record has no 'SomeUnexpectedField'");
}
($val, $msg) = $rec->SetSomeUnexpectedField( 'foo' );
ok(!$val, $msg);
is($msg, 'Nonexistant field?', "Field doesn't exist");
($val, $msg) = $rec->_Set('SomeUnexpectedField', 'foo');
ok(!$val, "$msg");


# Validate truncation on update

($val,$msg) = $rec->SetName('1234567890123456789012345678901234567890');

ok($val, $msg) ;

is($rec->Name, '12345678901234', "Truncated on update");



# Test unicode truncation:
my $univalue = "這是個測試";

($val,$msg) = $rec->SetName($univalue.$univalue);

ok($val, $msg) ;

is($rec->Name, '這是個測');



# make sure we do _not_ truncate things which should not be truncated
($val,$msg) = $rec->SetEmployeeId('1234567890');

ok($val, $msg) ;

is($rec->EmployeeId, '1234567890', "Did not truncate id on create");

# make sure we do truncation on create
my $newrec = TestApp::Address->new($handle);
my $newid = $newrec->Create( Name => '1234567890123456789012345678901234567890',
                             EmployeeId => '1234567890' );

$newrec->Load($newid);

ok ($newid, "Created a new record");
is($newrec->Name, '12345678901234', "Truncated on create");
is($newrec->EmployeeId, '1234567890', "Did not truncate id on create");



package TestApp::Address;

use base qw/DBIx::SearchBuilder::Record/;

sub _Init {
    my $self = shift;
    my $handle = shift;
    $self->Table('Address');
    $self->_Handle($handle);
}

sub _ClassAccessible {

    {   
        
        id =>
        {read => 1, type => 'int(11)', default => ''}, 
        Name => 
        {read => 1, write => 1, type => 'varchar(14)', default => ''},
        Phone => 
        {read => 1, write => 1, type => 'varchar(18)', default => ''},
        EmployeeId => 
        {read => 1, write => 1, type => 'int(8)', default => ''},

}

}


sub schema {

<<EOF;
CREATE TABLE Address (
        id  integer primary key,
        Name varchar(36),
        Phone varchar(18),
        EmployeeId int(8))
EOF

}

1;
