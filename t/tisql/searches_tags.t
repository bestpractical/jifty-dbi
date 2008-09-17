#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 299;

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
        my $count = init_data( 'TestApp::Node', $handle );
        ok( $count,  "init data" );
        $count = init_data( 'TestApp::Tag', $handle );
        ok( $count,  "init data" );
    }

    my $clean_obj = TestApp::NodeCollection->new( handle => $handle );
    my $nodes_obj = $clean_obj->clone;
    is_deeply( $nodes_obj, $clean_obj, 'after Clone looks the same');

    run_our_cool_tests(
        $nodes_obj,
        ".type = 'article'" => [qw(a aa at axy)],
        ".type = 'memo'"    => [qw(m mm mt mqwe)],

        # has [no] tags
        ".tags.id IS NULL"     => [qw(a m)],
        ".tags.id IS NOT NULL" => [qw(aa at axy mm mt mqwe)],
        "has no .tags"    => [qw(a m)],
        "has .tags" => [qw(aa at axy mm mt mqwe)],

        # has [no] tags and/or type
        ".tags.id IS NULL     AND .type = 'article'" => [qw(a)],
        ".tags.id IS NOT NULL AND .type = 'article'" => [qw(aa at axy)],
        ".tags.id IS NULL     OR  .type = 'article'" => [qw(a aa at axy m)],
        ".tags.id IS NOT NULL OR  .type = 'article'" => [qw(a aa at axy mm mt mqwe)],

        # tag = x
        ".tags.value = 'no'" => [qw()],
        ".tags.value = 'a'" => [qw(aa)],
        ".tags.value = 't'" => [qw(at mt)],
        ".tags.value LIKE '%'" => [qw(aa at axy mm mt mqwe)],

        # tag = x written as 'has .tags.value =x'
        "has .tags.value = 'no'" => [qw()],
        "has .tags.value = 'a'" => [qw(aa)],
        "has .tags.value = 't'" => [qw(at mt)],
        "has .tags.value LIKE '%'" => [qw(aa at axy mm mt mqwe)],

        # tag != x
        ".tags.value != 'no'" => [qw(a aa at axy m mm mt mqwe)],
        ".tags.value != 'a'" => [qw(a at axy m mm mt mqwe)],
        ".tags.value != 't'" => [qw(a aa axy m mm mqwe)],
        ".tags.value != 'q'" => [qw(a aa at axy m mm mt)],
        ".tags.value NOT LIKE '%'" => [qw(a m)],

        # tag = x and/or tag = y
        ".tags.value = 'no' AND .tags.value = 't'" => [qw()],
        ".tags.value = 'no' OR  .tags.value = 't'" => [qw(at mt)],
        ".tags.value = 'a' AND .tags.value = 't'" => [qw()],
        ".tags.value = 'a' OR  .tags.value = 't'" => [qw(aa at mt)],
        ".tags.value = 'x' AND .tags.value = 'y'" => [qw(axy)],
        ".tags.value = 'x' OR  .tags.value = 'y'" => [qw(axy)],

        # tag != x and/or tag = y
        ".tags.value != 'no' AND .tags.value = 't'" => [qw(at mt)],
        ".tags.value != 'no' OR  .tags.value = 't'" => [qw(a aa at axy m mm mt mqwe)],
        ".tags.value != 'a' AND .tags.value = 't'" => [qw(at mt)],
        ".tags.value != 'a' OR  .tags.value = 't'" => [qw(a at axy m mm mt mqwe)],
        ".tags.value != 'x' AND .tags.value = 'y'" => [qw()],
        ".tags.value != 'x' OR  .tags.value = 'y'" => [qw(a aa at axy m mm mt mqwe)],

        # tag != x and/or tag != y
        ".tags.value != 'no' AND .tags.value != 't'" => [qw(a aa axy m mm mqwe)],
        ".tags.value != 'no' OR  .tags.value != 't'" => [qw(a aa at axy m mm mt mqwe)],
        ".tags.value != 'a' AND .tags.value != 't'" => [qw(a axy m mm mqwe)],
        ".tags.value != 'a' OR  .tags.value != 't'" => [qw(a aa at axy m mm mt mqwe)],
        ".tags.value != 'x' AND .tags.value != 'y'" => [qw(a aa at m mm mt mqwe)],
        ".tags.value != 'x' OR  .tags.value != 'y'" => [qw(a aa at m mm mt mqwe)],

        # has .tag != x
        "has .tags.value != 'no'" => [qw(aa at axy mm mt mqwe)],
        "has .tags.value != 'a'" => [qw(at axy mm mt mqwe)],
        "has .tags.value != 't'" => [qw(aa axy mm mqwe)],
        "has .tags.value != 'q'" => [qw(aa at axy mm mt mqwe)],

        # has no .tag != x
        "has no .tags.value != 'no'" => [qw(a m)],
        "has no .tags.value != 'a'"  => [qw(a aa m)],
        "has no .tags.value != 't'"  => [qw(a at m mt)],
        "has no .tags.value != 'q'"  => [qw(a m)],

        # crazy things
### XXX, TODO, FIXME
        # get all nodes that have intersection in tags with article #3 (at)
#        ".tags.nodes.id = 3" => [qw(at mt)],
        # get all nodes that have intersactions in tags with nodes that have tag 't'
#        ".tags.nodes.tags.value = 't'" => [qw(at mt)],

    );

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks

sub run_our_cool_tests {
    my $collection = shift;
    my $bundling;
    $bundling = shift if @_ % 2;
    my %tests = @_;
    while (my ($q, $check) = each %tests ) {
        $check = { map {$_ => 1} @$check };
        $collection->clean_slate;
        $collection->tisql( joins_bundling => $bundling )->query( $q );
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
    return run_our_cool_tests( $collection, 1, %tests ) unless $bundling;
}
1;


package TestApp;
sub schema_sqlite { [
q{ CREATE table nodes (
    id integer primary key,
    type varchar(36),
    subject varchar(36)
) },
q{ CREATE table tags (
    id integer primary key,
    node integer not null,
    value varchar(36)
) },
] }

sub schema_mysql { [
q{ CREATE table nodes (
    id integer primary key auto_increment,
    type varchar(36),
    subject varchar(36)
) },
q{ CREATE table tags (
    id integer primary key auto_increment,
    node integer not null,
    value varchar(36)
) },
] }
sub cleanup_schema_mysql { [
    "DROP table tags", 
    "DROP table nodes", 
] }

package TestApp::TagCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

package TestApp::NodeCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

package TestApp::Tag;
use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';
# definition below

package TestApp::Node;
use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column type => type is 'varchar(36)';
    column subject => type is 'varchar(36)';
    column tags => refers_to TestApp::TagCollection by 'node';
};
}

sub init_data {
    return (
    [ 'type', 'subject' ],

    [ 'article', 'a' ],
    [ 'article', 'aa' ],
    [ 'article', 'at' ],
    [ 'article', 'axy' ],

    [ 'memo', 'm' ],
    [ 'memo', 'mm' ],
    [ 'memo', 'mt' ],
    [ 'memo', 'mqwe' ],
    );
}

package TestApp::Tag;

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column node => type is 'integer',
        refers_to TestApp::Node;
    column value => type is 'varchar(36)';
    column nodes => refers_to TestApp::NodeCollection
        by tisql => 'nodes.tags.value = .value';
};
}

sub init_data {
    return (
    [ 'node', 'value' ],

#    [ 1, 'article', 'a' ],
#    [ 2, 'article', 'aa' ],
    [ 2, 'a' ],
#    [ 3, 'article', 'at' ],
    [ 3, 't' ],
#    [ 4, 'article', 'axy' ],
    [ 4, 'x' ],
    [ 4, 'y' ],
#    [ 5, 'memo', 'm' ],
#    [ 6, 'memo', 'mm' ],
    [ 6, 'm' ],
#    [ 7, 'memo', 'mt' ],
    [ 7, 't' ],
#    [ 8, 'memo', 'mqwe' ],
    [ 8, 'q' ],
    [ 8, 'w' ],
    [ 8, 'e' ],
    );
}

