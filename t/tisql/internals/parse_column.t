#!/usr/bin/env perl -w

use strict;
use warnings;

use Data::Dumper;
use Test::More tests => 12;

BEGIN { require "t/utils.pl" }

my $tisql = TestApp::UserCollection->new->tisql;
isa_ok( $tisql => 'Jifty::DBI::Tisql');

sub parse_ok($$) {
    my ($str, $exp) = @_;
    $exp->{'string'} ||= $str;
    my $desc = "Parsed correctly column from '$str'";
    my $res = $tisql->parse_column($str);
    is_deeply($res, $exp, $desc)
        or diag "got: ". Dumper( $res ) ."expected: ". Dumper( $exp ); 
}

parse_ok ".col" => {
    alias   => '',
    chain   => [{ name => 'col', string => '.col' }],
};

parse_ok "alias.col" => {
    alias   => 'alias',
    chain   => [{ name => 'col', string => 'alias.col' }],
};

parse_ok ".col.id" => {
    alias   => '',
    chain   => [{ name => 'col', string => '.col' }, { name => 'id', string => '.col.id' }],
};

parse_ok "alias.col.id" => {
    alias   => 'alias',
    chain   => [{ name => 'col', string => 'alias.col' }, { name => 'id', string => 'alias.col.id' }],
};

# place holders
parse_ok ".col{'v'}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{'v'}", placeholders => [["'v'"]] }],
};

parse_ok ".col{'v1', 'v2'}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{'v1', 'v2'}", placeholders => [["'v1'", "'v2'"]] }],
};

parse_ok ".col{'v11', 'v12'}{'v21', 'v22'}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{'v11', 'v12'}{'v21', 'v22'}", placeholders => [["'v11'", "'v12'"], ["'v21'", "'v22'"]] }],
};

# bindings in placeholder
parse_ok ".col{?}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{?}", placeholders => ['?'] }],
};

parse_ok ".col{?}{?}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{?}{?}", placeholders => ['?', '?'] }],
};

parse_ok ".col{%1}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{%1}", placeholders => ['%1'] }],
};

parse_ok ".col{%1}{%3}" => {
    alias   => '',
    chain   => [{ name => 'col', string => ".col{%1}{%3}", placeholders => ['%1', '%3'] }],
};

1;


package TestApp;
sub schema_sqlite {
[
q{
CREATE table users (
    id integer primary key,
    login varchar(36)
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
]
}

sub schema_pg {
[
q{
CREATE TEMPORARY table users (
    id serial primary key,
    login varchar(36)
) },
]
}

sub schema_oracle { [
    "CREATE SEQUENCE users_seq",
    "CREATE table users (
        id integer CONSTRAINT users_Key PRIMARY KEY,
        login varchar(36)
    )",
] }

sub cleanup_schema_oracle { [
    "DROP SEQUENCE users_seq",
    "DROP table users", 
] }

package TestApp::User;

use base qw/Jifty::DBI::Record/;
our $VERSION = '0.01';

BEGIN {
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {
    column login => type is 'varchar(36)';
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

