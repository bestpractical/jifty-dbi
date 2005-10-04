package Sample::Employee;
use base qw/Jifty::DBI::Record/;

package Sample::Employee::Schema;
use Jifty::DBI::Schema;

column name      => type is 'varchar';
column dexterity => type is 'integer';



package Sample::Address;
use base qw/Jifty::DBI::Record/;

package Sample::Address::Schema;
use Jifty::DBI::Schema;

column name =>
  type is 'varchar',
  default is 'Frank';

column phone =>
  type is 'varchar';

column employee_id =>
  refers_to Sample::Employee;
