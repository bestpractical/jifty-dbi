#!/usr/bin/env perl -w

use strict;
use warnings;

use File::Spec;
use Test::More;

BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 6;

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
    my $count_groups = init_data( 'TestApp::Group', $handle );
    ok( $count_groups,  "init groups data" );
    my $count_us2gs = init_data( 'TestApp::UserToGroup', $handle );
    ok( $count_us2gs,  "init users&groups relations data" );

    my $clean_obj = TestApp::UserCollection->new( handle => $handle );
    my $users_obj = $clean_obj->clone;
    is_deeply( $users_obj, $clean_obj, 'after Clone looks the same');

    $users_obj->tisql('.login = "ivan" OR ( .login like "au%" AND .login not like "%n%" )');
    diag $users_obj->build_select_query;

    $users_obj->clean_slate;
    $users_obj->tisql('.gm.grp.name = "Support"');

    $users_obj->clean_slate;
    $users_obj->tisql('from .gm.grp as g1 where g1.name = "Support" or g1.name = "Developers"');

    cleanup_schema( 'TestApp', $handle );

}} # SKIP, foreach blocks

1;


package TestApp;
sub schema_sqlite {
[
q{
CREATE table users (
    id integer primary key,
    login varchar(36)
) },
q{
CREATE table user_to_groups (
    id integer primary key,
    person integer,
    grp integer
) },
q{
CREATE table groups (
    id integer primary key,
    name varchar(36)
) },
]
}

sub schema_mysql {
[
q{
CREATE TEMPORARY table users (
    id integer primary key AUTO_INCREMENT,
    login varchar(36)
) },
q{
CREATE TEMPORARY table user_to_groups (
    id integer primary key AUTO_INCREMENT,
    user_id  integer,
    group_id integer
) },
q{
CREATE TEMPORARY table groups (
    id integer primary key AUTO_INCREMENT,
    name varchar(36)
) },
]
}

sub schema_pg {
[
q{
CREATE TEMPORARY table users (
    id serial primary key,
    login varchar(36)
) },
q{
CREATE TEMPORARY table user_to_groups (
    id serial primary key,
    user_id integer,
    group_id integer
) },
q{
CREATE TEMPORARY table groups (
    id serial primary key,
    name varchar(36)
) },
]
}

sub schema_oracle { [
    "CREATE SEQUENCE users_seq",
    "CREATE table users (
        id integer CONSTRAINT users_Key PRIMARY KEY,
        login varchar(36)
    )",
    "CREATE SEQUENCE user_to_groups_seq",
    "CREATE table user_to_groups (
        id integer CONSTRAINT user_to_groups_Key PRIMARY KEY,
        user_id integer,
        group_id integer
    )",
    "CREATE SEQUENCE groups_seq",
    "CREATE table groups (
        id integer CONSTRAINT groups_Key PRIMARY KEY,
        name varchar(36)
    )",
] }

sub cleanup_schema_oracle { [
    "DROP SEQUENCE users_seq",
    "DROP table users", 
    "DROP SEQUENCE groups_seq",
    "DROP table groups", 
    "DROP SEQUENCE user_to_groups_seq",
    "DROP table user_to_groups", 
] }

package TestApp::User;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column login => type is 'varchar(36)';
    column gm    => refers_to TestApp::UserToGroupCollection by 'person', is virtual;
};
}

sub _init {
    my $self = shift;
    $self->table('users');
    $self->SUPER::_init( @_ );
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

package TestApp::Group;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column name => type is 'varchar(36)';
    column gm => refers_to TestApp::UserToGroupCollection by 'grp';
};
}

sub _init {
    my $self = shift;
    $self->table('groups');
    return $self->SUPER::_init( @_ );
}

sub init_data {
    return (
    [ 'name' ],

    [ 'Developers' ],
    [ 'Sales' ],
    [ 'Support' ],
    );
}

package TestApp::GroupCollection;

use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

sub _init {
    my $self = shift;
    $self->table('groups');
    return $self->SUPER::_init( @_ );
}

1;

package TestApp::UserToGroup;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column grp => refers_to TestApp::Group;
    column person => refers_to TestApp::User;
};
}

sub init_data {
    return (
    [ 'grp',    'person' ],
# dev group
    [ 1,        1 ],
    [ 1,        2 ],
    [ 1,        4 ],
# sales
#    [ 2,        0 ],
# support
    [ 3,        1 ],
    );
}

package TestApp::UserToGroupCollection;
use base qw/Jifty::DBI::Collection/;
our $VERSION = '0.01';

1;
