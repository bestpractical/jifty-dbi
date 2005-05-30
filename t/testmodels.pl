package Sample::Address;

use base qw/DBIx::SearchBuilder::Record/;

# Class and instance method

sub Table { "Addresses" }

# Class and instance method

sub TableDescription {
    return {
        Name => { TYPE => 'varchar', },
        Phone => { TYPE => 'varchar', },
#        EmployeeId => { REFERENCES => 'Sample::Employee', },
    }
}

1;