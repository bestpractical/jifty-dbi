#$Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/Pg.pm,v 1.8 2001/07/27 05:23:29 jesse Exp $
# Copyright 1999-2001 Jesse Vincent <jesse@fsck.com>

package DBIx::SearchBuilder::Handle::Pg;
use DBIx::SearchBuilder::Handle;
@ISA = qw(DBIx::SearchBuilder::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);


use strict;

# {{{ sub Connect
=head2 Connect

Connect takes a hashref and passes it off to SUPER::Connect;
Forces the timezone to GMT
it returns a database handle.

=cut
  
sub Connect {
    my $self = shift;
    
    $self->SUPER::Connect(@_);
    $self->SimpleQuery("SET TIME ZONE 'GMT'");
    $self->SimpleQuery("SET DATESTYLE TO 'ISO'");
    $self->AutoCommit(1);
    return ($DBIHandle); 
}
# }}}

# {{{ sub Insert

=head2 Insert

Takes a table name as the first argument and assumes that the rest of the arguments
are an array of key-value pairs to be inserted.

In case of isnert failure, returns a Class::ReturnValue object preloaded
with error info

=cut


sub Insert {
    my $self = shift;
    my $table = shift;
    
    my $sth = $self->SUPER::Insert($table, @_ );
    
    unless ($sth) {
	    return ($sth);
    }

    #Lets get the id of that row we just inserted    
    my $oid = $sth->{'pg_oid_status'};
    my $sql = "SELECT id FROM $table WHERE oid = ?";
    my @row = $self->FetchResult($sql, $oid);
    # TODO: Propagate Class::ReturnValue up here.
    unless ($row[0]) {
	    print STDERR "Can't find $table.id  for OID $oid";
	    return(undef);
    }	
    $self->{'id'} = $row[0];
    
    return ($self->{'id'});
}

# }}}

# {{{ BinarySafeBLOBs

=head2 BinarySafeBLOBs

Return undef, as no current version of postgres supports binary-safe blobs

=cut

sub BinarySafeBLOBs {
    my $self = shift;
    return(undef);
}

# }}}

=head2 ApplyLimits STATEMENTREF ROWS_PER_PAGE FIRST_ROW

takes an SQL SELECT statement and massages it to return ROWS_PER_PAGE starting with FIRST_ROW;


=cut

sub ApplyLimits {
    my $self = shift;
    my $statementref = shift;
    my $per_page = shift;
    my $first = shift;

    my $limit_clause = '';

    if ( $per_page) {
        $limit_clause = " LIMIT ";
        $limit_clause .= $per_page;
        if ( $first != 0 ) {
            $limit_clause .= " OFFSET $first";
        }
    }

   $$statementref .= $limit_clause; 

}

=head2 Join { Paramhash }

Takes a paramhash of everything Searchbuildler::Record does + a parameter called 'SearchBuilder that contains a ref to a SearchBuilder object'.
This performs the join.


=cut


sub Join {

    my $self = shift;
    my %args = (
        SearchBuilder => undef,
        TYPE          => 'normal',
        FIELD1        => undef,
        ALIAS1        => undef,
        TABLE2        => undef,
        FIELD2        => undef,
        ALIAS2        => undef,
        @_
    );

    my $string;

    my $alias;

#If we're handed in an ALIAS2, we need to go remove it from the Aliases array.
# Basically, if anyone generates an alias and then tries to use it in a join later, we want to be smart about
# creating joins, so we need to go rip it out of the old aliases table and drop it in as an explicit join
    if ( $args{'ALIAS2'} ) {

        # this code is slow and wasteful, but it's clear.
        my @aliases = @{ $args{'SearchBuilder'}->{'aliases'} };
        my @new_aliases;
        foreach my $old_alias (@aliases) {
            if ( $old_alias =~ /^(.*?) ($args{'ALIAS2'})$/ ) {
                $args{'TABLE2'} = $1;
                $alias = $2;

            }
            else {
                push @new_aliases, $old_alias;
            }
        }
        # If we found an alias, great. let's just pull out the table and alias for the other item
        unless ($alias) {
            # if we can't do that, can we reverse the join and have it work?
            my $a1 = $args{'ALIAS1'};
            my $f1 = $args{'FIELD1'};
            $args{'ALIAS1'} = $args{'ALIAS2'};
            $args{'FIELD1'} = $args{'FIELD2'};
            $args{'ALIAS2'} = $a1;
            $args{'FIELD2'} = $f1;

            @aliases     = @{ $args{'SearchBuilder'}->{'aliases'} };
            @new_aliases = ();
            foreach my $old_alias (@aliases) {
                if ( $old_alias =~ /^(.*?) ($args{'ALIAS2'})$/ ) {
                    $args{'TABLE2'} = $1;
                    $alias = $2;

                }
                else {
                    push @new_aliases, $old_alias;
                }
            }

        }

        unless ($alias) {
            return ( $self->SUPER::Join(%args) );
        }
        if ($args{'ALIAS1'}) {
            return ( $self->SUPER::Join(%args) );
        }

        $args{'SearchBuilder'}->{'aliases'} = \@new_aliases;
    }

    else {
        $alias = $args{'SearchBuilder'}->_GetAlias( $args{'TABLE2'} );

    }

    if ( $args{'TYPE'} =~ /LEFT/i ) {

        $string = " LEFT JOIN " . $args{'TABLE2'} . " as $alias ";

    }
    else {

        $string = " JOIN " . $args{'TABLE2'} . " as $alias ";

    }
    $args{'SearchBuilder'}->{'left_joins'}{"$alias"}{'alias_string'} = $string;
    $args{'SearchBuilder'}->{'left_joins'}{"$alias"}{'depends_on'}   = $args{'ALIAS1'};
    $args{'SearchBuilder'}->{'left_joins'}{"$alias"}{'criteria'} { 'criterion' . $args{'SearchBuilder'}->{'criteria_count'}++ } = " $args{'ALIAS1'}.$args{'FIELD1'} = $alias.$args{'FIELD2'}";

    return ($alias);
}



# this code is all hacky and evil. but people desperately want _something_ and I'm 
# super tired. refactoring gratefully appreciated.

sub _BuildJoins {
    my $self = shift;
    my $sb   = shift;
    my %seen_aliases;

    $seen_aliases{'main'} = 1;

    my $join_clause =$sb->{'table'} . " main " ;

    my @keys = ( keys %{ $sb->{'left_joins'} } );

    while ( my $join = shift @keys ) {
        if ( $seen_aliases{ $sb->{'left_joins'}{$join}{'depends_on'} } ) {
            $join_clause  = "(" . $join_clause;
            $join_clause .= $sb->{'left_joins'}{$join}{'alias_string'} . " ON (";
            $join_clause .=
              join ( ') AND( ', values %{ $sb->{'left_joins'}{$join}{'criteria'} }
              );
            $join_clause .= ")) ";

            $seen_aliases{$join} = 1;
        }
        else {
            push ( @keys, $join );
        }

    }
    return (
                    join ( ", ", ($join_clause, @{ $sb->{'aliases'} }))) ;

}

1;
