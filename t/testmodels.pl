package Sample::Address;

use base qw/DBIx::SearchBuilder::Record/;

# Class and instance method

sub Table { "Addresses" }

# Class and instance method

sub Schema {
    return {
        Name => { TYPE => 'varchar', DEFAULT => 'Frank', },
        Phone => { TYPE => 'varchar', },
        EmployeeId => { REFERENCES => 'Sample::Employee', },
    }
}

package Sample::Employee;

use base qw/DBIx::SearchBuilder::Record/;

sub Table { "Employees" }

sub Schema {
    return {
      Name => { TYPE => 'varchar', },
      Dexterity => { TYPE => 'integer', },
    }
}

1;