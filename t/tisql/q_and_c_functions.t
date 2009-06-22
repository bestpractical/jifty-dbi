#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 31; # 485;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

use Data::Dumper;

use Jifty::DBI::Tisql qw(Q C);

my $clean;

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
        $count = init_data( 'TestApp::Attribute', $handle );
        ok( $count,  "init data" );
    }

    $clean = TestApp::NodeCollection->new( handle => $handle );
    my $nodes_obj = $clean->clone;
    is_deeply( $nodes_obj, $clean, 'after Clone looks the same');

    run_our_cool_tests(
        $clean,
        ".type = 'article'" => Q(type => 'article'),
        ".type = 'memo'"    => Q(type => 'memo'),
        ".attrs.id IS NULL" => Q(C(qw(attrs id)) => 'IS NULL'),
        "has no .attrs"     => Q('has no' => 'attrs'),

        ".attrs{ name => 'a'}.id IS NULL" => Q(C('attrs', {name => 'a'}, 'id') => 'IS NULL'),
        "has no .attrs{name => 'a'}"      => Q('has no' => C('attrs', {name => 'a'})),
        "has no .attrs{name => 'a', 'b'}" => Q('has no' => C('attrs', {name => ['a', 'b']})),

        ".attrs.id IS NOT NULL" => Q(C(qw(attrs id)) => 'IS NOT NULL'),
        "has .attrs"            => Q(has => 'attrs'),

        ".attrs{name => 'a'}.id IS NOT NULL" => Q(C('attrs', {name => 'a'}, 'id') => 'IS NOT NULL'),
        "has .attrs{name => 'a'}"            => Q(has => C('attrs', {name => 'a'})),
        "has .attrs{name => 'b','c'}"        => Q(has => C('attrs', {name => ['b', 'c']})),

        ".attrs.value = 'no'"                      => Q(C(qw(attrs value)) => 'no'),
        ".attrs.value = 'a'"                       => Q(C(qw(attrs value)) => 'a'),
        ".attrs{name => 'a'}.value = 'a'"          => Q(C(attrs => {name => 'a'}, 'value') => 'a'),
        ".attrs{name => 'a', 'b'}.value = 'c'"     => Q(C(attrs => {name => ['a', 'b']}, 'value') => 'c'),
        "has .attrs.value = 'no'"                  => Q(has => C(qw(attrs value)) => 'no'),
        "has .attrs.value = 'a'"                   => Q(has => C(qw(attrs value)) => 'a'),
        "has .attrs{name => 'a'}.value = 'a'"      => Q(has => C(attrs => {name => 'a'}, 'value') => 'a'),
        "has .attrs{name => 'a', 'b'}.value = 'c'" => Q(has => C(attrs => {name => ['a', 'b']}, 'value') => 'c'),

        ".attrs.value != 'no'"                   => Q(C(qw(attrs value)), '!=', 'no'),
        ".attrs.value != 'a'"                    => Q(C(qw(attrs value)), '!=', 'a'),
        ".attrs{name => 'a'}.value != 'a'"       => Q(C(attrs => {name => 'a'}, 'value'), '!=', 'a'),
        "has no .attrs.value = 'no'"             => Q('has no', C(qw(attrs value)) => 'no'),
        "has no .attrs.value = 'a'"              => Q('has no' => C(qw(attrs value)) => 'a'),
        "has no .attrs{name => 'a'}.value = 'a'" => Q('has no' => C(attrs => {name => 'a'}, 'value'), '=', 'a'),
    );

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks


sub run_our_cool_tests {
    my $clean = shift;
    my (@tests) = @_;
    while (my ($qstring, $qstruct) = splice @tests, 0, 2 ) {
        my $collection_string = $clean->clone;
        $collection_string->tisql->query( $qstring );

        my $collection_struct = $clean->clone;
        $collection_struct->tisql->query( $qstruct );

        is_deeply(
            $collection_string, $collection_struct,
            'collections built from string and struct are equal'
        );
    }
}

1;


package TestApp;
sub schema_sqlite { [
q{ CREATE table nodes (
    id integer primary key,
    type varchar(36),
    subject varchar(36)
) },
q{ CREATE table attributes (
    id integer primary key,
    node integer not null,
    name varchar(36),
    value varchar(36)
) },
] }

# definitions below
package TestApp::AttributeCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

package TestApp::NodeCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

package TestApp::Attribute;
use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

package TestApp::Node;
use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column type => type is 'varchar(36)';
    column subject => type is 'varchar(36)';
    column attrs => refers_to TestApp::AttributeCollection
        by tisql => "attrs.node = .id AND attrs.name = %name";
};
}

sub init_data {
    return (
    [ 'type', 'subject' ],

    [ 'article', 'a-' ],
    [ 'article', 'aaa' ],
    [ 'article', 'aab' ],
    [ 'article', 'aac' ],
    [ 'article', 'aaaab' ],
    [ 'article', 'aabac' ],
    [ 'article', 'aacaa' ],
    [ 'article', 'aba' ],
    [ 'article', 'abb' ],
    [ 'article', 'abc' ],


    [ 'memo', 'm-' ],
    [ 'memo', 'maa' ],
    [ 'memo', 'mab' ],
    [ 'memo', 'mac' ],
    [ 'memo', 'maaab' ],
    [ 'memo', 'mabac' ],
    [ 'memo', 'macaa' ],
    [ 'memo', 'mba' ],
    [ 'memo', 'mbb' ],
    [ 'memo', 'mbc' ],

    [ 'article', 'acc' ],
    [ 'memo', 'mcc' ],
    );
}

package TestApp::Attribute;

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column node => type is 'integer',
        refers_to TestApp::Node;
    column name  => type is 'varchar(36)';
    column value => type is 'varchar(36)';
    column nodes => refers_to TestApp::NodeCollection
        by tisql => 'nodes.attrs.value = .value';
};
}

sub init_data {
    return (
    [ 'node', 'name', 'value' ],

#   [1, 'article', 'a-' ],
#   [2, 'article', 'aaa' ],
    [2, 'a', 'a' ],
#   [3, 'article', 'aab' ],
    [3, 'a', 'b' ],
#   [4, 'article', 'aac' ],
    [4, 'a', 'c' ],
#   [5, 'article', 'aaaab' ],
    [5, 'a', 'a' ],
    [5, 'a', 'b' ],
#   [6, 'article', 'aabac' ],
    [6, 'a', 'b' ],
    [6, 'a', 'c' ],
#   [7, 'article', 'aacaa' ],
    [7, 'a', 'c' ],
    [7, 'a', 'a' ],
#   [8, 'article', 'aba' ],
    [8, 'b', 'a' ],
#   [9, 'article', 'abb' ],
    [9, 'b', 'b' ],
#   [10, 'article', 'abc' ],
    [10, 'b', 'c' ],
 
 
#   [11, 'memo', 'm-' ],
#   [12, 'memo', 'maa' ],
    [12, 'a', 'a' ],
#   [13, 'memo', 'mab' ],
    [13, 'a', 'b' ],
#   [14, 'memo', 'mac' ],
    [14, 'a', 'c' ],
#   [15, 'memo', 'maaab' ],
    [15, 'a', 'a' ],
    [15, 'a', 'b' ],
#   [16, 'memo', 'mabac' ],
    [16, 'a', 'b' ],
    [16, 'a', 'c' ],
#   [17, 'memo', 'macaa' ],
    [17, 'a', 'c' ],
    [17, 'a', 'a' ],
#   [18, 'memo', 'mba' ],
    [18, 'b', 'a' ],
#   [19, 'memo', 'mbb' ],
    [19, 'b', 'b' ],
#   [20, 'memo', 'mbc' ],
    [20, 'b', 'c' ],

#   [21, 'article', 'acc' ],
    [21, 'c', 'c' ],
#   [22, 'memo', 'mcc' ],
    [22, 'c', 'c' ],
    );
}

