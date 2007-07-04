#!/usr/bin/env perl 

use strict;
use warnings;
use Test::More;
use version;

use constant TESTS_PER_DRIVER => 48;
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
    my $address_schema = has_schema('Sample::Address',$d);
    my $employee_schema = has_schema('Sample::Employee',$d);
    unless ($address_schema && $employee_schema) {
      skip "need to work on $d", TESTS_PER_DRIVER;
    }
    
    unless( should_test( $d ) ) {
        skip "ENV is not defined for driver $d", TESTS_PER_DRIVER;
    }

    # Test that declarative schema syntax automagically sets validators
    # correctly.
    ok( Sample::Address->can('validate_name'), 'found validate_name' );
    my $validator = Sample::Address->column('name')->validator;
    ok( $validator, 'found $column->validator' );
    is( $validator, \&Sample::Address::validate_name, 'validators match' );

    my $handle = get_handle( $d );
    connect_handle( $handle );
    isa_ok($handle, "Jifty::DBI::Handle::$d");
    isa_ok($handle->dbh, 'DBI::db');

    my $SG = Jifty::DBI::SchemaGenerator->new($handle);

    isa_ok($SG, 'Jifty::DBI::SchemaGenerator');

    isa_ok($SG->_db_schema, 'DBIx::DBSchema');

    is($SG->create_table_sql_text, '', "no tables means no sql");

    my $ret = $SG->add_model('Sample::This::Does::Not::Exist');

    ok(not ($ret), "couldn't add model from nonexistent class");

    like($ret->error_message, qr/Error making new object from Sample::This::Does::Not::Exist/, 
      "couldn't add model from nonexistent class");

    is($SG->create_table_sql_text, '', "no tables means no sql");

    $ret = $SG->add_model('Sample::Address');

    ok($ret != 0, "added model from real class");

    is_ignoring_space($SG->create_table_sql_text, 
                      Sample::Address->$address_schema,
                      "got the right Address schema for $d");

    my $employee = Sample::Employee->new;
    
    isa_ok($employee, 'Sample::Employee');
    can_ok($employee, qw( label type dexterity ));
    
    $ret = $SG->add_model($employee);

    ok($ret != 0, "added model from an instantiated object");

    is_ignoring_space($SG->create_table_sql_text, 
                      Sample::Address->$address_schema. Sample::Employee->$employee_schema, 
                      "got the right Address+Employee schema for $d");
    
    my $manually_make_text = join ' ', map { "$_;" } $SG->create_table_sql_statements;
     is_ignoring_space($SG->create_table_sql_text, 
                       $manually_make_text, 
                       'create_table_sql_text is the statements in create_table_sql_statements');

    my $version_024_min = version->new('0.2.4');
    my $version_024_max = version->new('0.2.8');

    for my $version (qw/ 0.2.0 0.2.4 0.2.6 0.2.8 0.2.9 /) {

        Sample::Address->schema_version($version);

        my $SG = Jifty::DBI::SchemaGenerator->new($handle, $version);
        $SG->add_model('Sample::Address');

        my $street_added
            = version->new($version) >= $version_024_min
           && version->new($version) <  $version_024_max;

        ok(Sample::Address->COLUMNS->{id}->active, 'id active');
        ok(Sample::Address->COLUMNS->{employee_id}->active, 'employee_id active');
        ok(Sample::Address->COLUMNS->{name}->active, 'name active');
        ok(Sample::Address->COLUMNS->{phone}->active, 'phone active');
        if ($street_added) {
            ok(Sample::Address->COLUMNS->{street}->active, 'street active');
        }

        else {
            ok(!Sample::Address->COLUMNS->{street}->active, 'street not active');
        }

        my $address_version_schema = $street_added ? "${address_schema}_024"
            :                                         $address_schema;

        is_ignoring_space($SG->create_table_sql_text,
                        Sample::Address->$address_version_schema,
                        "got the right Address schema for $d version $version");
    }

    cleanup_schema( 'TestApp', $handle );
    disconnect_handle( $handle );
}
}

sub is_ignoring_space {
    my $a = shift;
    my $b = shift;

    for my $item ( $b, $a ) {
        $item =~ s/^\s+//;
        $item =~ s/\s+$//;
        $item =~ s/\s+/ /g;
        $item =~ s/\s+;/;/g;
        $item =~ s/\(\s+(.*?)\s+\)/($1)/g;

        unshift @_, $item;
    }
    goto &is;
}
