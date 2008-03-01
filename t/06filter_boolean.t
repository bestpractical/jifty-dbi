#!/usr/bin/env perl
use strict;
use warnings;

use Test::More;
BEGIN { require "t/utils.pl" }
our (@available_drivers);

use constant TESTS_PER_DRIVER => 130;

my $total = scalar(@available_drivers) * TESTS_PER_DRIVER;
plan tests => $total;

my @true  = qw/1 t true y yes TRUE/;
my @false = qw/0 f false n no FALSE/;

foreach my $d (@available_drivers) {
SKIP: {
    unless (has_schema('TestApp::User', $d)) {
        skip "No schema for '$d' driver", TESTS_PER_DRIVER;
    }

    unless (should_test($d)) {
        skip "ENV is not defined for driver '$d'", TESTS_PER_DRIVER;
    }

    diag("start testing with '$d' handle") if $ENV{TEST_VERBOSE};

    my $handle = get_handle($d);
    connect_handle($handle);
    isa_ok($handle->dbh, 'DBI::db');

    {
        my $ret = init_schema('TestApp::User', $handle);
        isa_ok($ret, 'DBI::st', 'init schema');
    }

    my @values = (
        ( map { [$_, 'true']  } @true  ),
        ( map { [$_, 'false'] } @false ),
    );

    for my $value ( @values, [undef, 'false'] ) {
        my ($input, $bool) = @$value;

        my $rec = TestApp::User->new( handle => $handle );
        isa_ok($rec, 'Jifty::DBI::Record');

        my ($id) = $rec->create( defined($input) ? (my_data => $input) : () );
        ok($id, 'created record');
        ok($rec->load($id), 'loaded record');
        is($rec->id, $id, 'record id matches');

        is($rec->my_data, $bool eq 'true' ? 1 : 0, 'Perl agrees with the expected boolean value');

        if ($d eq 'Pg') {
            # this option tells DBD::Pg to keep booleans as 't' and 'f' and not
            # map them to 1 and 0
            $handle->dbh->{pg_bool_tf} = 1;
        }

        my $sth = $handle->simple_query("SELECT my_data FROM users WHERE id = $id");
        my ($got) = $sth->fetchrow_array;

        my $method = "canonical_$bool";
        is( $got, $handle->$method, "my_data bool match for " . (defined($input) ? $input : 'undef') . " ($bool)" );

        if ($d eq 'Pg') {
            $handle->dbh->{pg_bool_tf} = 0;
        }

        # undef/NULL
        $rec->set_my_data;
        is($rec->my_data, undef, 'set undef value');

        $rec->set_my_data($input);
        ok($bool eq 'true' ? $rec->my_data : !$rec->my_data, 'Perl agrees with the expected boolean value');
    }

    for my $value ( @values ) {
        my ($input, $bool) = @$value;
        my $rec = TestApp::User->new( handle => $handle );
        $rec->load_by_cols(
            my_data => $input,
        );
        ok($rec->id, "loaded a record by boolean value '$input'");

        my $col = TestApp::UserCollection->new( handle => $handle );
        $col->limit(
            column => 'my_data',
            value  => $input,
        );
        if ($col->count) {
            ok($bool eq 'true' ? $col->first->my_data : !$col->first->my_data, 'Perl agrees with the expected boolean value');
        }
        else {
            fail("Got no results from limit");
        }
    }

    cleanup_schema('TestApp', $handle);
    disconnect_handle($handle);
}
}

package TestApp::User;
use base qw/ Jifty::DBI::Record /;

sub schema_sqlite {

<<EOF;
CREATE table users (
    id integer primary key,
    my_data boolean
)
EOF

}

sub schema_mysql {

<<EOF;
CREATE TEMPORARY table users (
    id integer auto_increment primary key,
    my_data boolean
)
EOF

}

sub schema_pg {

<<EOF;
CREATE TEMPORARY table users (
    id serial primary key,
    my_data boolean
)
EOF

}

BEGIN {
    use Jifty::DBI::Schema;

    use Jifty::DBI::Record schema {
    column my_data =>
        is boolean;
    }
}

package TestApp::UserCollection;

use base qw/Jifty::DBI::Collection/;

sub _init {
    my $self = shift;
    $self->SUPER::_init(@_);
    $self->table('users');
}

