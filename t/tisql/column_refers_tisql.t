#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 21;

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

    my $count_users = init_data( 'TestApp::User', $handle );
    ok( $count_users,  "init users data" );
    my $count_attributes = init_data( 'TestApp::Attribute', $handle );
    ok( $count_attributes,  "init attributes data" );

# ivan
{
    my $user = TestApp::User->load('ivan', _handle => $handle);
    ok $user->id, "loaded ivan's record";
    my $attrs = $user->attrs;
    $attrs->order_by(column => 'value', order => 'asc');
    is $attrs->count, 1, "found one ivan's attribute";
    is $attrs->next->value, 'foo', "correct value";
    is $attrs->next, undef, "correct value";
}

# john
{
    my $user = TestApp::User->load('john', _handle => $handle);
    ok $user->id, "loaded john's record";
    my $attrs = $user->attrs;
    $attrs->order_by(column => 'value', order => 'asc');
    is $attrs->count, 1, "found one john's attribute";
    is $attrs->next->value, 'bar', "correct value";
    is $attrs->next, undef, "correct value";
}

# bob
{
    my $user = TestApp::User->load('bob', _handle => $handle);
    ok $user->id, "loaded bob's record";
    my $attrs = $user->attrs;
    $attrs->order_by(column => 'value', order => 'asc');
    is $attrs->count, 3, "found three bob's attribute"
        or diag "bad sql query: ". $attrs->build_select_query;
    is $attrs->next->value, 'bar', "correct value";
    is $attrs->next->value, 'foo', "correct value";
    is $attrs->next->value, 'zoo', "correct value";
    is $attrs->next, undef, "correct value";
}

# aurelia
{
    my $user = TestApp::User->load('aurelia', _handle => $handle);
    ok $user->id, "loaded aurelia's record";
    my $attrs = $user->attrs;
    $attrs->order_by(column => 'value', order => 'asc');
    is $attrs->count, 0, "havn't found aurelia's attributes";
    is $attrs->next, undef, "correct value";
}
    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks

1;


package TestApp;
sub schema_sqlite {
[
q{
CREATE TABLE users (
    id integer primary key,
    login varchar(36)
) },
q{
CREATE TABLE attributes (
    id integer primary key,
    model varchar(36),
    record integer,
    value varchar(36)
) },
]
}

package TestApp::Attribute;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column model => type is 'varchar(36)';
    column record => type is 'integer';
    column value => type is 'varchar(36)';
};
}

sub _init {
    my $self = shift;
    $self->table('attributes');
    return $self->SUPER::_init( @_ );
}

sub init_data {
    return (
    [ 'model', 'record', 'value' ],

    [ 'User', '1', 'foo' ], # ivan
    [ 'User', '2', 'bar' ], # john
    [ 'User', '3', 'foo' ], # bob
    [ 'User', '3', 'bar' ], # bob
    [ 'User', '3', 'zoo' ], # bob
# no attributes for aurelia
# some attributes for 'Group' records matching users' ids
    [ 'Group', '1', 'wrong' ],
    [ 'Group', '2', 'wrong' ],
    [ 'Group', '3', 'wrong' ],
    [ 'Group', '4', 'wrong' ],
    );
}

package TestApp::AttributeCollection;

use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

sub _init {
    my $self = shift;
    $self->table('attributes');
    return $self->SUPER::_init( @_ );
}

1;
package TestApp::User;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column login => type is 'varchar(36)';
    column attrs =>
        refers_to TestApp::AttributeCollection
            by tisql => "attrs.model = 'User' AND attrs.record = .id",
        is virtual;
};
}

sub _init {
    my $self = shift;
    $self->table('users');
    $self->SUPER::_init( @_ );
}

sub load {
    my $self= shift;
    my $ident = shift;
    return $self->SUPER::load( $ident, @_ )
        if $ident =~ /^\d+$/;
    return $self->load_by_cols(
        login => $ident,
        @_
    );
}


sub init_data {
    return (
    [ 'login' ],

    [ 'ivan' ],
    [ 'john' ],
    [ 'bob' ],
    [ 'aurelia' ],
    );
}

package TestApp::UserCollection;

use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

sub _init {
    my $self = shift;
    $self->table('users');
    return $self->SUPER::_init( @_ );
}

1;
