package Sample::Employee;
use base qw/Jifty::DBI::Record/;

package Sample::Employee::Schema;
use Jifty::DBI::Schema;

column dexterity => type is 'integer';
column name      => type is 'varchar';



package Sample::Address;
use base qw/Jifty::DBI::Record/;

package Sample::Address::Schema;
use Jifty::DBI::Schema;

column employee_id =>
  refers_to Sample::Employee;

column name =>
  type is 'varchar',
  default is 'Frank';

column phone =>
  type is 'varchar';

1;
