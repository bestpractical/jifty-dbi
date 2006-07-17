package Sample::Employee;
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {

column dexterity => type is 'integer';
column name      => type is 'varchar';
column label     => type is 'varchar';
column type      => type is 'varchar';

};

package Sample::Address;
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {

column employee_id =>
  refers_to Sample::Employee;

column name =>
  type is 'varchar',
  default is 'Frank';

column phone =>
  type is 'varchar';

};

1;
