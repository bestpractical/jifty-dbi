
package DBIx::SearchBuilder::Handle::SQLite;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  DBIx::SearchBuilder::Handle::SQLite -- a mysql specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

perl(1), DBIx::SearchBuilder

=cut

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.


If the insert succeeds, returns the id of the insert, otherwise, returns
a Class::ReturnValue object with the error reploaded.

=cut

sub Insert  {
    my $self = shift;
    my $table = shift;
    my %args = ( id => undef, @_);
    # We really don't want an empty id
    
    my $sth = $self->SUPER::Insert($table, %args);
    if (!$sth) {
	    return ($sth);
     }

    # If we have set an id, then we want to use that, otherwise, we want to lookup the last _new_ rowid
    $self->{'id'}= $args{'id'} || $self->dbh->func('last_insert_rowid');

    warn "$self no row id returned on row creation" unless ($self->{'id'});
    return( $self->{'id'}); #Add Succeded. return the id
  }

# }}}


=head2 CaseSensitive 

Returns undef, since mysql's searches are not case sensitive by default 

=cut

sub CaseSensitive {
    my $self = shift;
    return(1);
}

sub BinarySafeBLOBs { 
    return undef;
}

# }}}

=head2 DistinctCount STATEMENTREF

takes an incomplete SQL SELECT statement and massages it to return a DISTINCT result count


=cut

sub DistinctCount {
    my $self = shift;
    my $statementref = shift;

    # Wrapper select query in a subselect as Oracle doesn't allow
    # DISTINCT against CLOB/BLOB column types.
    $$statementref = "SELECT count(*) FROM (SELECT DISTINCT main.id FROM $$statementref )";

}

# }}}



#SQLite can't handle 
# SELECT DISTINCT main.*     FROM (Groups main          LEFT JOIN Principals Principals_2  ON ( main.id = Principals_2.id)) ,     GroupMembers GroupMembers_1      WHERE ((GroupMembers_1.MemberId = '70'))     AND ((Principals_2.Disabled = '0'))     AND ((main.Domain = 'UserDefined'))     AND ((main.id = GroupMembers_1.GroupId)) 
#     ORDER BY main.Name ASC
#     It needs
#SELECT DISTINCT main.*     FROM Groups main           LEFT JOIN Principals Principals_2  ON ( main.id = Principals_2.id) ,      GroupMembers GroupMembers_1      WHERE ((GroupMembers_1.MemberId = '70'))     AND ((Principals_2.Disabled = '0'))     AND ((main.Domain = 'UserDefined'))     AND ((main.id = GroupMembers_1.GroupId)) ORDER BY main.Name ASC



sub _BuildJoins {
    my $self = shift;
    my $sb   = shift;
    my %seen_aliases;
    
    $seen_aliases{'main'} = 1;

    # We don't want to get tripped up on a dependency on a simple alias. 
        foreach my $alias ( @{ $sb->{'aliases'}} ) {
          if ( $alias =~ /^(.*?)\s+(.*?)$/ ) {
              $seen_aliases{$2} = 1;
          }
    }

    my $join_clause = $sb->{'table'} . " main ";
    
    my @keys = ( keys %{ $sb->{'left_joins'} } );
    my %seen;
    
    while ( my $join = shift @keys ) {
        if ( ! $sb->{'left_joins'}{$join}{'depends_on'} || $seen_aliases{ $sb->{'left_joins'}{$join}{'depends_on'} } ) {
           #$join_clause = "(" . $join_clause;
            $join_clause .=
              $sb->{'left_joins'}{$join}{'alias_string'} . " ON (";
            $join_clause .=
              join ( ') AND( ',
                values %{ $sb->{'left_joins'}{$join}{'criteria'} } );
            $join_clause .= ") ";
            
            $seen_aliases{$join} = 1;
        }   
        else {
            push ( @keys, $join );
            die "Unsatisfied dependency chain in Joins @keys"
              if $seen{"@keys"}++;
        }     
        
    }
    return ( join ( ", ", ( $join_clause, @{ $sb->{'aliases'} } ) ) );
    
}


1;
