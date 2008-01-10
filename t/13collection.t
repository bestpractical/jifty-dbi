# Test methods in Jifty::DBI::Collection.

use strict;
use warnings;

use Test::More tests => 7;

my $package;
BEGIN { 
    $package = 'Jifty::DBI::Collection';
    use_ok($package);
}

#
# Test the _order_clause method
#

my $obj = bless {
    order_by => [
      {
        alias  => 'main',
        column => 'name',
        order  => 'desc',
      },
      {
        alias  => 'foo',
        column => 'id',
        order  => 'des',
      },
      {
        alias  => 'bar',
        column => 'msg_session',
        order  => 'DesC',
      }
    ],
}, $package;

is $obj->_order_clause,
   ' ORDER BY main.name DESC, foo.id DESC, bar.msg_session DESC ',
   'desc works';

##

$obj = bless {
    order_by => [
      {
        alias  => 'messages',
        column => 'name',
        order  => 'asc',
      },
      {
        alias  => 'QQUsers',
        column => 'sent',
        order  => 'ASC',
      },
      {
        alias  => 'stu_dents',
        column => 'msg_session',
        order  => 'AsC',
      }
    ],
}, $package;

is $obj->_order_clause,
   ' ORDER BY messages.name ASC, QQUsers.sent ASC, stu_dents.msg_session ASC ',
   'asc works';

##

$obj = bless {
    order_by => [
      {
        alias  => '',
        column => 'name',
      },
      {
        alias  => 0,
        column => 'sent',
      },
      {
        alias  => 'ab',
        column => 'msg_session',
      }
    ],
}, $package;

is $obj->_order_clause,
   ' ORDER BY name ASC, sent ASC, ab.msg_session ASC ',
   'empty and false aliases';

$obj->add_order_by(
    {
        alias => 'ab',
        column => 'msg_id',
        order => 'DESC',
    },
    {
        alias => 'main',
        column => 'yaks',
    },
);

is $obj->_order_clause,
   ' ORDER BY name ASC, sent ASC, ab.msg_session ASC, ab.msg_id DESC, main.yaks ASC ',
   "add_order_by doesn't thrash previous ordering";

$obj->order_by(
        alias => 'ab',
        column => 'msg_id',
        order => 'DESC',
);

is $obj->_order_clause,
   ' ORDER BY ab.msg_id DESC ',
   "order_by does thrash previous ordering";

$obj->add_order_by(
        alias => 'main',
        column => 'yaks',
);

is $obj->_order_clause,
   ' ORDER BY ab.msg_id DESC, main.yaks ASC ',
   "add_order_by works when passing a list-as-hash directly";

