#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 32;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

use Data::Dumper;

foreach my $d ( @available_drivers ) {
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
    isa_ok($ret, 'DBI::st', "Inserted the schema. got a statement handle back");

    {
        my $count = init_data( 'TestApp::Task', $handle );
        ok( $count,  "init data" );
        $count = init_data( 'TestApp::Link', $handle );
        ok( $count,  "init data" );
    }

    my $clean_obj = TestApp::TaskCollection->new( handle => $handle );
    my $tasks_obj = $clean_obj->clone;
    is_deeply( $tasks_obj, $clean_obj, 'after Clone looks the same');

    run_our_cool_tests(
        $tasks_obj,
        ".links_to.dst_id = 2" => [qw(1_m_of_2)],
        ".links_to{type => 'member_of'}.dst_id = 2" => [qw(1_m_of_2)],
        ".links_to{model => 'task'}.dst_id = 2" => [qw(1_m_of_2)],
        ".links_to{type => 'member_of'}{model => 'task'}.dst_id = 2" => [qw(1_m_of_2)],
        ".links_from{type => 'member_of'}{model => 'task'}.src_id = 1" => [qw(2_has_m_1)],
        ".linked_tasks.subject = '2_has_m_1'" => [qw(1_m_of_2)],
        ".linked_to_tasks.subject = '2_has_m_1'" => [qw(1_m_of_2)],
        ".linked_from_tasks.subject = '1_m_of_2'" => [qw(2_has_m_1)],
        ".member_of.subject = '2_has_m_1'" => [qw(1_m_of_2)],
    );

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks


sub run_our_cool_tests {
    my $collection = shift;
    my %tests = @_;
    while (my ($q, $check) = each %tests ) {
        $check = { map {$_ => 1} @$check };
        $collection->clean_slate;
        $collection->tisql->query( $q );
        my $expected_count = scalar grep $_, values %$check;
        is($collection->count, $expected_count, "count is correct for $q")
            or diag "wrong count query: ". $collection->build_select_count_query;
       
        my @not_expected;
        while (my $item = $collection->next ) {
            my $t = $item->subject;
            push @not_expected, $t unless $check->{ $t };
            delete $check->{ $t };
        }
        my $fault = 0;
        $fault = 1 if @not_expected;
        ok !@not_expected, "didn't find additionals for $q"
            or diag "found not expected: ". join ', ', @not_expected;

        $fault = 1 if keys %$check;
        ok !keys %$check, "found all expected for $q"
            or diag "didn't find expected: ". join ', ', keys %$check;

        diag "wrong select query: ". $collection->build_select_query
            if $fault;
    }
}
1;


package TestApp;
sub schema_sqlite { [
q{ CREATE table tasks (
    id integer primary key,
    subject varchar(32)
) },
q{ CREATE table links (
    id integer primary key,
    src_model varchar(32) not null,
    src_id integer not null,
    type varchar(32) not null,
    dst_model varchar(32) not null,
    dst_id integer not null
) },
] }

sub schema_mysql { [
q{ CREATE table tasks (
    id integer primary key AUTO_INCREMENT,
    subject varchar(32)
) },
q{ CREATE table links (
    id integer primary key AUTO_INCREMENT,
    src_model varchar(32) not null,
    src_id integer not null,
    type varchar(32) not null,
    dst_model varchar(32) not null,
    dst_id integer not null
) },
] }
sub cleanup_schema_mysql { [
    "DROP table tasks", 
    "DROP table links", 
] }

# definitions below to avoid problems with interdependencies
package TestApp::LinkCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

package TestApp::TaskCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

package TestApp::Link;
use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

package TestApp::Task;
use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column subject => type is 'varchar(32)';

    column links => refers_to TestApp::LinkCollection
        by tisql => 'links.type = %type AND ((links.src_model = "task" AND links.src_id = .id)
            OR (links.dst_model = "task" AND dst_id = .id))';
    column links_from => refers_to TestApp::LinkCollection
        by tisql => 'links_from.dst_model = "task" AND links_from.dst_id = .id'
            .' AND links_from.type = %type AND links_from.src_model = %model';
    column links_to => refers_to TestApp::LinkCollection
        by tisql => 'links_to.src_model = "task" AND links_to.src_id = .id'
            .' AND links_to.type = %type AND links_to.dst_model = %model';

    column linked_tasks => refers_to TestApp::TaskCollection
        by tisql => 'linked_tasks.id = .links_from{type => %type}{model => "task"}.src_id'
            .' OR linked_tasks.id = .links_to{type => %type}{model => "task"}.dst_id';

    column linked_to_tasks => refers_to TestApp::TaskCollection
        by tisql => 'linked_to_tasks.id = .links_to{type => %type}{model => "task"}.dst_id';
    column linked_from_tasks => refers_to TestApp::TaskCollection
        by tisql => 'linked_from_tasks.id = .links_from{type => %type}{model => "task"}.src_id';

    column member_of => refers_to TestApp::TaskCollection
        by tisql => 'member_of.id = .links_to{type => "member_of"}{model => "task"}.dst_id';
};

sub init_data {
    return (
        ['subject'],
        ['1_m_of_2'],
        ['2_has_m_1'],
    );
}

package TestApp::Link;

use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column src_model => type is 'varchar(32)';
    column src_id    => type is 'integer';
    column type      => type is 'varchar(32)';
    column dst_model => type is 'varchar(32)';
    column dst_id    => type is 'integer';
};

sub init_data {
    return (
        ['src_model', 'src_id', 'type', 'dst_model', 'dst_id'],
        ['task', 1, 'member_of', 'task', 2],
    );
}


