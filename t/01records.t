#!/usr/bin/perl -w


use strict;
use warnings;
use File::Spec;
use Test::More;
BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 65;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

foreach my $d ( @available_drivers ) {
SKIP: {
	unless( has_schema( 'TestApp::Address', $d ) ) {
		skip "No schema for '$d' driver", TESTS_PER_DRIVER;
	}
	unless( should_test( $d ) ) {
		skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
	}

	my $handle = get_handle( $d );
	connect_handle( $handle );
	isa_ok($handle->dbh, 'DBI::db');

	my $ret = init_schema( 'TestApp::Address', $handle );
	isa_ok($ret,'DBI::st', "Inserted the schema. got a statement handle back");

	my $rec = TestApp::Address->new($handle);
	isa_ok($rec, 'Jifty::DBI::record');

# _Accessible testings
	is( $rec->_accessible('id' => 'read'), 1, 'id is accessible for read' );
	is( $rec->_accessible('id' => 'write'), undef, 'id is not accessible for write' );
	is( $rec->_accessible('id'), undef, "any field is not accessible in undefined mode" );
	is( $rec->_accessible('unexpected_field' => 'read'), undef, "field doesn't exist and can't be accessible for read" );
	is_deeply( [sort($rec->readable_attributes)], [qw(employee_id name phone id)], 'readable attributes' );
	is_deeply( [sort($rec->writable_attributes)], [qw(employee_id name phone)], 'writable attributes' );

	can_ok($rec,'create');

	my ($id) = $rec->create( name => 'Jesse', phone => '617 124 567');
	ok($id,"Created record ". $id);
	ok($rec->load($id), "Loaded the record");


	is($rec->id, $id, "The record has its id");
	is ($rec->name, 'Jesse', "The record's name is Jesse");

	my ($val, $msg) = $rec->set_name('Obra');
	ok($val, $msg) ;
	is($rec->name, 'Obra', "We did actually change the name");

# Validate immutability of the field id
	($val, $msg) = $rec->set_id( $rec->id + 1 );
	ok(!$val, $msg);
	is($msg, 'Immutable field', 'id is immutable field');
	is($rec->id, $id, "The record still has its id");

# Check some non existant field
	ok( !eval{ $rec->some_unexpected_field }, "The record has no 'some_unexpected_field'");
	{
		# test produce DBI warning
		local $SIG{__WARN__} = sub {return};
		is( $rec->_Value( 'some_unexpected_field' ), undef, "The record has no 'some_unexpected_field'");
	}
	($val, $msg) = $rec->set_some_unexpected_field( 'foo' );
	ok(!$val, $msg);
	is($msg, 'Nonexistant field?', "Field doesn't exist");
	($val, $msg) = $rec->_set('some_unexpected_field', 'foo');
	ok(!$val, "$msg");


# Validate truncation on update

	($val,$msg) = $rec->set_name('1234567890123456789012345678901234567890');
	ok($val, $msg);
	is($rec->name, '12345678901234', "Truncated on update");
	$val = $rec->truncate_value(phone => '12345678901234567890');
	is($val, '123456789012345678', 'truncate by length attribute');


# Test unicode truncation:
	my $univalue = "這是個測試";
	($val,$msg) = $rec->set_name($univalue.$univalue);
	ok($val, $msg) ;
	is($rec->name, '這是個測');



# make sure we do _not_ truncate things which should not be truncated
	($val,$msg) = $rec->set_employee_id('1234567890');
	ok($val, $msg) ;
	is($rec->employee_id, '1234567890', "Did not truncate id on create");

# make sure we do truncation on create
	my $newrec = TestApp::Address->new($handle);
	my $newid = $newrec->create( name => '1234567890123456789012345678901234567890',
	                             employee_id => '1234567890' );

	$newrec->load($newid);

	ok ($newid, "Created a new record");
	is($newrec->name, '12345678901234', "Truncated on create");
	is($newrec->employee_id, '1234567890', "Did not truncate id on create");

# no prefetch feature and _LoadFromSQL sub checks
	$newrec = TestApp::Address->new($handle);
	($val, $msg) = $newrec->_load_from_sql('SELECT id FROM address WHERE id = ?', $newid);
	is($val, 1, 'found object');
	is($newrec->name, '12345678901234', "autoloaded not prefetched field");
	is($newrec->employee_id, '1234567890', "autoloaded not prefetched field");

# _LoadFromSQL and missing PK
	$newrec = TestApp::Address->new($handle);
	($val, $msg) = $newrec->_load_from_sql('SELECT name FROM address WHERE name = ?', '12345678901234');
	is($val, 0, "didn't find object");
	is($msg, "Missing a primary key?", "reason is missing PK");

# _LoadFromSQL and not existant row
	$newrec = TestApp::Address->new($handle);
	($val, $msg) = $newrec->_load_from_sql('SELECT id FROM address WHERE id = ?', 0);
	is($val, 0, "didn't find object");
	is($msg, "Couldn't find row", "reason is wrong id");

# _load_from_sql and wrong SQL
	$newrec = TestApp::Address->new($handle);
	{
		local $SIG{__WARN__} = sub{return};
		($val, $msg) = $newrec->_load_from_sql('SELECT ...');
	}
	is($val, 0, "didn't find object");
	is($msg, "Couldn't execute query", "reason is bad SQL");

# test Load* methods
	$newrec = TestApp::Address->new($handle);
	$newrec->load();
	is( $newrec->id, undef, "can't load record with undef id");

	$newrec = TestApp::Address->new($handle);
	$newrec->load_by_col( name => '12345678901234' );
	is( $newrec->id, $newid, "load record by 'name' column value");

# LoadByCol with operator
	$newrec = TestApp::Address->new($handle);
	$newrec->load_by_col( name => { value => '%45678%',
				      operator => 'LIKE' } );
	is( $newrec->id, $newid, "load record by 'name' with LIKE");

# LoadByPrimaryKeys
	$newrec = TestApp::Address->new($handle);
	($val, $msg) = $newrec->load_by_primary_keys( id => $newid );
	ok( $val, "load record by PK");
	is( $newrec->id, $newid, "loaded correct record");
	$newrec = TestApp::Address->new($handle);
	($val, $msg) = $newrec->load_by_primary_keys( {id => $newid} );
	ok( $val, "load record by PK");
	is( $newrec->id, $newid, "loaded correct record" );
	$newrec = TestApp::Address->new($handle);
	($val, $msg) = $newrec->load_by_primary_keys( phone => 'some' );
	ok( !$val, "couldn't load, missing PK field");
	is( $msg, "Missing PK field: 'id'", "right error message" );

# LoadByCols and empty or NULL values
	$rec = TestApp::Address->new($handle);
	$id = $rec->create( name => 'Obra', phone => undef );
	ok( $id, "new record");
	$rec = TestApp::Address->new($handle);
	$rec->load_by_cols( name => 'Obra', phone => undef, employee_id => '' );
    is( $rec->id, $id, "loaded record by empty value" );

# __Set error paths
	$rec = TestApp::Address->new($handle);
	$rec->load( $id );
	$val = $rec->set_name( 'Obra' );
	isa_ok( $val, 'Class::ReturnValue', "couldn't set same value, error returned");
	is( ($val->as_array)[1], "That is already the current value", "correct error message" );
	is( $rec->name, 'Obra', "old value is still there");
	$val = $rec->set_name( 'invalid' );
	isa_ok( $val, 'Class::ReturnValue', "couldn't set invalid value, error returned");
	is( ($val->as_array)[1], 'Illegal value for Name', "correct error message" );
	is( $rec->name, 'Obra', "old value is still there");
# XXX TODO FIXME: this test cover current implementation that is broken //RUZ
	$val = $rec->set_name( );
	isa_ok( $val, 'Class::ReturnValue', "couldn't set empty/undef value, error returned");
	is( ($val->as_array)[1], "No value passed to _set", "correct error message" );
	is( $rec->name, 'Obra', "old value is still there");

# deletes
	$newrec = TestApp::Address->new($handle);
	$newrec->load( $newid );
	is( $newrec->delete, 1, 'successfuly delete record');
	$newrec = TestApp::Address->new($handle);
	$newrec->load( $newid );
	is( $newrec->id, undef, "record doesn't exist any more");

	cleanup_schema( 'TestApp::Address', $handle );
}} # SKIP, foreach blocks

1;



package TestApp::Address;

use base qw/Jifty::DBI::Record/;

sub _init {
    my $self = shift;
    my $handle = shift;
    $welf->table('address');
    $self->_handle($handle);
}

sub validate_name
{
	my ($self, $value) = @_;
	return 0 if $value =~ /invalid/i;
	return 1;
}

sub _class_accessible {

    {   
        
        id =>
        {read => 1, type => 'int(11)', default => ''}, 
        name => 
        {read => 1, write => 1, type => 'varchar(14)', default => ''},
        phone => 
        {read => 1, write => 1, type => 'varchar(18)', length => 18, default => ''},
        employee_id => 
        {read => 1, write => 1, type => 'int(8)', default => ''},

}

}

sub schema_mysql {
<<EOF;
CREATE TEMPORARY TABLE address (
        id integer AUTO_INCREMENT,
        name varchar(36),
        phone varchar(18),
        employee_id int(8),
  	PRIMARY KEY (id))
EOF

}

sub schema_pg {
<<EOF;
CREATE TEMPORARY TABLE address (
        id serial PRIMARY KEY,
        name varchar,
        phone varchar,
        employee_id integer
)
EOF

}

sub schema_sqlite {

<<EOF;
CREATE TABLE address (
        id  integer primary key,
        name varchar(36),
        phone varchar(18),
        employee_id int(8))
EOF

}

1;
