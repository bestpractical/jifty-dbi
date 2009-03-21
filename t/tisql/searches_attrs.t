#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 485;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

use Data::Dumper;

use Jifty::DBI::Tisql qw(Q C);

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

    my $clean_obj = TestApp::NodeCollection->new( handle => $handle );
    my $nodes_obj = $clean_obj->clone;
    is_deeply( $nodes_obj, $clean_obj, 'after Clone looks the same');

    run_our_cool_tests(
        $nodes_obj,
        ".type = 'article'"  => [qw(a- aaa aab aac aaaab aabac aacaa aba abb abc acc)],
        Q(type => 'article') => [qw(a- aaa aab aac aaaab aabac aacaa aba abb abc acc)],
        ".type = 'memo'"     => [qw(m- maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        Q(type => 'memo')    => [qw(m- maa mab mac maaab mabac macaa mba mbb mbc mcc)],

        # has no attrs
        ".attrs.id IS NULL"             => [qw(a- m-)],
        Q(C(qw(attrs id)) => 'IS NULL') => [qw(a- m-)],
        "has no .attrs"                 => [qw(a- m-)],
        Q('has no' => 'attrs')          => [qw(a- m-)],

        ".attrs{ name => 'a'}.id IS NULL"               => [qw(a- m- aba abb abc acc mba mbb mbc mcc)],
        Q(C('attrs', {name => 'a'}, 'id') => 'IS NULL') => [qw(a- m- aba abb abc acc mba mbb mbc mcc)],
        "has no .attrs{name => 'a'}"                    => [qw(a- m- aba abb abc acc mba mbb mbc mcc)],
        Q('has no' => C('attrs', {name => 'a'}))        => [qw(a- m- aba abb abc acc mba mbb mbc mcc)],
        "has no .attrs{name => 'a', 'b'}"               => [qw(a- m- acc mcc)],
        Q('has no' => C('attrs', {name => ['a', 'b']})) => [qw(a- m- acc mcc)],

        # has attrs
        ".attrs.id IS NOT NULL"
            => [qw(aaa aab aac aaaab aabac aacaa aba abb abc acc maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        Q(C(qw(attrs id)) => 'IS NOT NULL')
            => [qw(aaa aab aac aaaab aabac aacaa aba abb abc acc maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        "has .attrs"      => [qw(aaa aab aac aaaab aabac aacaa aba abb abc acc maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        Q(has => 'attrs') => [qw(aaa aab aac aaaab aabac aacaa aba abb abc acc maa mab mac maaab mabac macaa mba mbb mbc mcc)],

        ".attrs{name => 'a'}.id IS NOT NULL"                => [qw(aaa aab aac aaaab aabac aacaa maa mab mac maaab mabac macaa)],
        Q(C('attrs', {name => 'a'}, 'id') => 'IS NOT NULL') => [qw(aaa aab aac aaaab aabac aacaa maa mab mac maaab mabac macaa)],
        "has .attrs{name => 'a'}"                           => [qw(aaa aab aac aaaab aabac aacaa maa mab mac maaab mabac macaa)],
        Q(has => C('attrs', {name => 'a'}))                 => [qw(aaa aab aac aaaab aabac aacaa maa mab mac maaab mabac macaa)],
        "has .attrs{name => 'b','c'}"                       => [qw(aba abb abc acc mba mbb mbc mcc)],
        Q(has => C('attrs', {name => ['b', 'c']}))          => [qw(aba abb abc acc mba mbb mbc mcc)],

        # attr = x
        ".attrs.value = 'no'"                                      => [qw()],
        Q(C(qw(attrs value)) => 'no')                              => [qw()],
        ".attrs.value = 'a'"                                       => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        Q(C(qw(attrs value)) => 'a')                               => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        ".attrs{name => 'a'}.value = 'a'"                          => [qw(aaa aaaab aacaa maa maaab macaa)],
        Q(C(attrs => {name => 'a'}, 'value') => 'a')               => [qw(aaa aaaab aacaa maa maaab macaa)],
        ".attrs{name => 'a', 'b'}.value = 'c'"                     => [qw(aac aacaa aabac abc mac macaa mabac mbc)],
        Q(C(attrs => {name => ['a', 'b']}, 'value') => 'c')        => [qw(aac aacaa aabac abc mac macaa mabac mbc)],
        "has .attrs.value = 'no'"                                  => [qw()],
        Q(has => C(qw(attrs value)) => 'no')                       => [qw()],
        "has .attrs.value = 'a'"                                   => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        Q(has => C(qw(attrs value)) => 'a')                        => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        "has .attrs{name => 'a'}.value = 'a'"                      => [qw(aaa aaaab aacaa maa maaab macaa)],
        Q(has => C(attrs => {name => 'a'}, 'value') => 'a')        => [qw(aaa aaaab aacaa maa maaab macaa)],
        "has .attrs{name => 'a', 'b'}.value = 'c'"                 => [qw(aac aacaa aabac abc mac macaa mabac mbc)],
        Q(has => C(attrs => {name => ['a', 'b']}, 'value') => 'c') => [qw(aac aacaa aabac abc mac macaa mabac mbc)],

        # attr != x
        ".attrs.value != 'no'"
            => [qw(a- aaa aab aac aaaab aabac aacaa aba abb abc acc m- maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        Q(C(qw(attrs value)), '!=', 'no')
            => [qw(a- aaa aab aac aaaab aabac aacaa aba abb abc acc m- maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        ".attrs.value != 'a'"
            => [qw(a- aab aac aabac abb abc acc m- mab mac mabac mbb mbc mcc)],
        Q(C(qw(attrs value)), '!=', 'a')
            => [qw(a- aab aac aabac abb abc acc m- mab mac mabac mbb mbc mcc)],
        ".attrs{name => 'a'}.value != 'a'"
            => [qw(a- aab aac aabac aba abb abc acc m- mab mac mabac mba mbb mbc mcc)],
        Q(C(attrs => {name => 'a'}, 'value'), '!=', 'a')
            => [qw(a- aab aac aabac aba abb abc acc m- mab mac mabac mba mbb mbc mcc)],
        "has no .attrs.value = 'no'"
            => [qw(a- aaa aab aac aaaab aabac aacaa aba abb abc acc m- maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        Q('has no', C(qw(attrs value)) => 'no')
            => [qw(a- aaa aab aac aaaab aabac aacaa aba abb abc acc m- maa mab mac maaab mabac macaa mba mbb mbc mcc)],
        "has no .attrs.value = 'a'"
            => [qw(a- aab aac aabac abb abc acc m- mab mac mabac mbb mbc mcc)],
        Q('has no' => C(qw(attrs value)) => 'a')
            => [qw(a- aab aac aabac abb abc acc m- mab mac mabac mbb mbc mcc)],
        "has no .attrs{name => 'a'}.value = 'a'"
            => [qw(a- aab aac aabac aba abb abc acc m- mab mac mabac mba mbb mbc mcc)],
        Q('has no' => C(attrs => {name => 'a'}, 'value'), '=', 'a')
            => [qw(a- aab aac aabac aba abb abc acc m- mab mac mabac mba mbb mbc mcc)],

        # attr = x and/or attr = y
        ".attrs.value = 'no' AND .attrs.value = 'a'" => [qw()],
        Q(C(qw(attrs value)) => 'no') & Q(C(qw(attrs value)) => 'a') => [qw()],
        ".attrs.value = 'no' OR  .attrs.value = 'a'" => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        Q(C(qw(attrs value)) => 'no') | Q(C(qw(attrs value)) => 'a') => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        ".attrs.value = 'a' AND .attrs.value = 'b'" => [qw(aaaab maaab)],
        Q(C(qw(attrs value)) => 'a') & Q(C(qw(attrs value)) => 'b') => [qw(aaaab maaab)],
        ".attrs.value = 'a' OR  .attrs.value = 'b'" => [qw(aaa aaaab aacaa aba maa maaab macaa mba aab abb mab mbb aabac mabac)],
        Q(C(qw(attrs value)) => 'a') | Q(C(qw(attrs value)) => 'b') => [qw(aaa aaaab aacaa aba maa maaab macaa mba aab abb mab mbb aabac mabac)],
        ".attrs{name => 'a'}.value = 'a' AND .attrs{name => 'b'}.value = 'b'" => [qw()],
        Q(C(attrs => {name => 'a'}, 'value') => 'a') & Q(C(attrs => {name => 'b'}, 'value') => 'b') => [qw()],
        ".attrs{name => 'a'}.value = 'a' OR .attrs{name => 'b'}.value = 'b'" => [qw(aaa aaaab aacaa abb maa maaab macaa mbb)],
        Q(C(attrs => {name => 'a'}, 'value') => 'a') | Q(C(attrs => {name => 'b'}, 'value') => 'b') => [qw(aaa aaaab aacaa abb maa maaab macaa mbb)],


        # tag != x and/or tag = y
        ".attrs.value != 'no' AND .attrs.value = 'a'"                    => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        Q(C(qw(attrs value)), '!=', 'no') & Q(C(qw(attrs value)) => 'a') => [qw(aaa aaaab aacaa aba maa maaab macaa mba)],
        ".attrs.value != 'no' OR  .attrs.value = 'a'"
            => [qw(a- aab aac aabac abb abc m- mab mac mabac mbb mbc acc mcc aaa aaaab aacaa aba maa maaab macaa mba)],
        Q(C(qw(attrs value)), '!=', 'no') | Q(C(qw(attrs value)) => 'a')
            => [qw(a- aab aac aabac abb abc m- mab mac mabac mbb mbc acc mcc aaa aaaab aacaa aba maa maaab macaa mba)],
        ".attrs.value != 'a' AND .attrs.value = 'b'"                    => [qw(abb mbb aab aabac mab mabac)],
        Q(C(qw(attrs value)), '!=', 'a') & Q(C(qw(attrs value)) => 'b') => [qw(abb mbb aab aabac mab mabac)],
        ".attrs.value != 'a' OR  .attrs.value = 'b'"
            => [qw(a- aab aac aabac abb abc m- mab mac mabac mbb mbc acc mcc aaaab maaab)],
        Q(C(qw(attrs value)), '!=', 'a') | Q(C(qw(attrs value)) => 'b')
            => [qw(a- aab aac aabac abb abc m- mab mac mabac mbb mbc acc mcc aaaab maaab)],
        ".attrs{name => 'a'}.value != 'a' AND .attrs{name => 'b'}.value = 'b'"                          => [qw(abb mbb)],
        Q(C(attrs => {name => 'a'}, 'value'), '!=', 'a') & Q(C(attrs => {name => 'b'}, 'value') => 'b') => [qw(abb mbb)],
        ".attrs{name => 'a'}.value != 'a' OR .attrs{name => 'b'}.value = 'b'"
            => [qw(a- aab aac aabac aba abc m- mab mac mabac mba mbc acc mcc abb mbb)],
        Q(C(attrs => {name => 'a'}, 'value'), '!=', 'a') | Q(C(attrs => {name => 'b'}, 'value') => 'b')
            => [qw(a- aab aac aabac aba abc m- mab mac mabac mba mbc acc mcc abb mbb)],

        # has .tag != x
        "has .attrs.value != 'a'" => [qw(aab aac aaaab aabac aacaa abb abc mab mac maaab mabac macaa mbb mbc acc mcc)],
        Q(has => C(attrs => 'value'), '!=', 'a') => [qw(aab aac aaaab aabac aacaa abb abc mab mac maaab mabac macaa mbb mbc acc mcc)],
        "has .attrs{name => 'a'}.value != 'b'" => [qw(aaa aac aaaab aabac aacaa maa mac maaab mabac macaa)],
        Q(has => C(attrs => {name => 'a'}, 'value'), '!=', 'b') => [qw(aaa aac aaaab aabac aacaa maa mac maaab mabac macaa)],

#        # has no .tag != x
#        "has no .attrs.value != 'no'" => [qw(a m)],
#        "has no .attrs.value != 'a'"  => [qw(a aa m)],
#        "has no .attrs.value != 't'"  => [qw(a at m mt)],
#        "has no .attrs.value != 'q'"  => [qw(a m)],
#

    );

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks


sub run_our_cool_tests {
    my $collection = shift;
    my $bundling;
    $bundling = shift if @_ % 2;
    my (@tmp, @tests);
    @tmp = @tests = @_;
    while (my ($q, $check) = splice @tests, 0, 2 ) {
#        use Data::Dumper;
#        diag( Dumper $q );
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

    return run_our_cool_tests( $collection, 1, @tmp ) unless $bundling;
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

