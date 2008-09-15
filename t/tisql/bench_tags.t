#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 1;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

my @types = qw(article memo note);
my @tags = qw(foo bar baz ball box apple orange fruit juice pearl gem briliant qwe asd zxc qwerty ytr dsa cxz boo bla);
my $total_objs = 30000;
my $max_tags = 3;
my $time_it = -10;

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
        $handle->dbh->do("CREATE INDEX tags1 ON tags(value, node)");
        $handle->dbh->do("CREATE INDEX tags2 ON tags(node, value)");
    }

    my $clean_obj = TestApp::NodeCollection->new( handle => $handle );
    my $nodes_obj = $clean_obj->clone;
    is_deeply( $nodes_obj, $clean_obj, 'after Clone looks the same');

    run_our_cool_tests(
        $nodes_obj, $handle,
        '.tags.value = "foo" OR .tags.value = "bar"',
        '.tags.value != "foo" AND .tags.value != "bar"',

    );

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks


use Benchmark qw(cmpthese);
sub run_our_cool_tests {
    my $collection = shift;
    my $handle = shift;
    my @tests = @_;
    foreach my $t ( @tests ) {
        diag "without bundling: ". do {
            $collection->clean_slate;
            my $tisql = $collection->tisql;
            $tisql->{'joins_bundling'} = 0;
            $tisql->query( $t );
            $collection->build_select_query;
        };
        diag "with    bundling: ". do {
            $collection->clean_slate;
            my $tisql = $collection->tisql;
            $tisql->{'joins_bundling'} = 1;
            $tisql->query( $t );
            $collection->build_select_query;
        };
        cmpthese( $time_it, {
            "  $t" => sub { 
                my $collection = TestApp::NodeCollection->new( handle => $handle );
                my $tisql = $collection->tisql;
                $tisql->{'joins_bundling'} = 0;
                $tisql->query( $t );
                $collection->next;
            },
            "b $t" => sub { 
                my $collection = TestApp::NodeCollection->new( handle => $handle );
                my $tisql = $collection->tisql;
                $tisql->{'joins_bundling'} = 1;
                $tisql->query( $t );
                $collection->next;
            } }
        );
    }
}
1;


package TestApp;
#sub schema_sqlite { [
#q{ CREATE table nodes (
#    id integer primary key,
#    type varchar(36),
#    subject varchar(36)
#) },
#q{ CREATE table tags (
#    id integer primary key,
#    node integer not null,
#    value varchar(36)
#) },
#] }

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

my @xxx = ('a'..'z');
sub init_data {
    my @res = (
        [ 'type', 'subject' ],
    );
    foreach ( 1 .. $total_objs ) {
        push @res, [ $types[ int rand @types ], $xxx[ int rand @xxx ] ];
    }
    return @res;
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
    my @res = (
        [ 'node', 'value' ],
    );
    foreach my $o ( 1 .. $total_objs ) {
        my $add = int rand $max_tags;
        my %added;
        while ( $add-- ) {
            my $tag;
            do {
                $tag = $tags[ int rand @tags ];
            } while $added{ $tag }++;
            push @res, [ $o, $tag ];
        }
    }
    return @res;
}

