package Example::Model::Employee;

use base qw/Jifty::DBI::Record/;

sub Table { "Employees" }

sub Schema {
    return {
      Name => { TYPE => 'varchar', },
      Dexterity => { TYPE => 'integer', },
    }
}

1;