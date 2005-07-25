package Sample::Address;

use base qw/Jifty::DBI::Record/;

# Class and instance method

sub Table { "addresses" }

# Class and instance method

sub schema {
    return {
        name => { TYPE => 'varchar', DEFAULT => 'Frank', },
        phone => { TYPE => 'varchar', },
        employee_id => { REFERENCES => 'Sample::Employee', },
    }
}

package Sample::Employee;

use base qw/Jifty::DBI::Record/;

sub table { "employees" }

sub schema {
    return {
      name => { TYPE => 'varchar', },
      dexterity => { TYPE => 'integer', },
    }
}

1;
