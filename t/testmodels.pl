package Sample::Employee;
use Jifty::DBI::Schema;
use Jifty::DBI::Record schema {

column dexterity => type is 'integer';
column name      => 
    type is 'varchar',
    is indexed;
column label     => type is 'varchar';
column type      => type is 'varchar';

};

sub schema_sqlite {
    return q{
    CREATE TABLE employees (
      id INTEGER PRIMARY KEY NOT NULL  ,
      dexterity integer   ,
      name varchar   ,
      label varchar   ,
      type varchar
    ) ;
    CREATE INDEX employees1 ON employees (name) ;
    };
}

sub schema_pg {
    return q{
    CREATE TABLE employees (
      id serial NOT NULL ,
      dexterity integer ,
      name varchar ,
      label varchar ,
      type varchar ,
      PRIMARY KEY (id)
    ) ;
    CREATE INDEX employees1 ON employees (name) ;
    };

}

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

sub validate_name { 1 }

sub schema_sqlite {
    return q{
    CREATE TABLE addresses (
     id INTEGER PRIMARY KEY NOT NULL  ,
     employee_id integer   ,
     name varchar  DEFAULT 'Frank' ,
     phone varchar
    ) ;
    }
}

sub schema_pg {
    return q{
    CREATE TABLE addresses ( 
      id serial NOT NULL , 
      employee_id integer  ,
      name varchar DEFAULT 'Frank' ,
      phone varchar ,
      PRIMARY KEY (id)
    ) ;
    };
}

1;
