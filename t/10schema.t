#!/usr/bin/perl

use strict;
use warnings;
use Test::More;

use constant TESTS_PER_DRIVER => 14;
our @available_drivers;

BEGIN {
  require("t/utils.pl");
  my $total = 3 + scalar(@available_drivers) * TESTS_PER_DRIVER;
  if( not eval { require DBIx::DBSchema } ) {
    plan skip_all => "DBIx::DBSchema not installed";
  } else {
    plan tests => $total;
  }
}

BEGIN { 
  use_ok("Jifty::DBI::SchemaGenerator");
  use_ok("Jifty::DBI::Handle");
}

require_ok("t/testmodels.pl");

foreach my $d ( @available_drivers ) {
  SKIP: {
    unless ($d eq 'Pg') {
      skip "first goal is to work on Pg", TESTS_PER_DRIVER;
    }
    
    unless( should_test( $d ) ) {
    	skip "ENV is not defined for driver $d", TESTS_PER_DRIVER;
    }
  
    my $handle = get_handle( $d );
    connect_handle( $handle );
    isa_ok($handle, "Jifty::DBI::Handle::$d");
    isa_ok($handle->dbh, 'DBI::db');

    my $SG = Jifty::DBI::SchemaGenerator->new($handle);

    isa_ok($SG, 'Jifty::DBI::SchemaGenerator');

    isa_ok($SG->_db_schema, 'DBIx::DBSchema');

    is($SG->create_table_sql_text, '', "no tables means no sql");

    my $ret = $SG->add_model('Sample::This::Does::Not::Exist');

    ok($ret == 0, "couldn't add model from nonexistent class");

    like($ret->error_message, qr/Error making new object from Sample::This::Does::Not::Exist/, 
      "couldn't add model from nonexistent class");

    is($SG->create_table_sql_text, '', "no tables means no sql");

    $ret = $SG->add_model('Sample::Address');

    ok($ret != 0, "added model from real class");

    is_ignoring_space($SG->create_table_sql_text, <<END_SCHEMA, "got the right schema");
    CREATE table addresses ( 
      id serial NOT NULL , 
      employee_id integer ,
      name varchar DEFAULT 'Frank' ,
      phone varchar ,
      PRIMARY KEY (id)
    ) ;
END_SCHEMA

    my $employee = Sample::Employee->new;
    
    isa_ok($employee, 'Sample::Employee');
    
    $ret = $SG->add_model($employee);

    ok($ret != 0, "added model from an instantiated object");

    is_ignoring_space($SG->create_table_sql_text, <<END_SCHEMA, "got the right schema");
    CREATE table addresses ( 
      id serial NOT NULL , 
      employee_id integer  ,
      name varchar DEFAULT 'Frank' ,
      phone varchar ,
      PRIMARY KEY (id)
    ) ;
    CREATE table employees (
      id serial NOT NULL ,
      dexterity integer ,
      name varchar ,
      PRIMARY KEY (id)
    ) ;
END_SCHEMA
    
    my $manually_make_text = join ' ', map { "$_;" } $SG->create_table_sql_statements;
    is_ignoring_space($SG->create_table_sql_text, $manually_make_text, 'create_table_sql_text is the statements in create_table_sql_statements')
}}

sub is_ignoring_space {
  my $a = shift;
  my $b = shift;
  
  $a =~ s/^\s+//; $a =~ s/\s+$//; $a =~ s/\s+/ /g;
  $b =~ s/^\s+//; $b =~ s/\s+$//; $b =~ s/\s+/ /g;
  
  unshift @_, $b; unshift @_, $a;
  
  goto &is;
}
