#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 2;

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
        by tisql => '((links.src_model = "Task" AND links.src_id = .id)
            OR (links.dst_model = "Task" AND dst_id = .id))';
    column to_links => refers_to TestApp::LinkCollection
        by tisql => '(links.dst_model = "Task" AND dst_id = .id)';
    column from_links => refers_to TestApp::LinkCollection
        by tisql => '(links.src_model = "Task" AND src_id = .id)';
};

sub init_data {
    return (
        ['subject'],
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
    );
}


