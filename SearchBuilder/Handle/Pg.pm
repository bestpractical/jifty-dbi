#$Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Handle/Pg.pm,v 1.8 2001/07/27 05:23:29 jesse Exp $
# Copyright 1999-2001 Jesse Vincent <jesse@fsck.com>

package DBIx::SearchBuilder::Handle::Pg;
use strict;

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use base qw(DBIx::SearchBuilder::Handle);
use Want qw(want);

use strict;

=head1 NAME

  DBIx::SearchBuilder::Handle::Pg - A Postgres specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of DBIx::SearchBuilder::Handle that 
compensates for some of the idiosyncrasies of Postgres.

=head1 METHODS

=cut

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
        if ( $first && $first != 0 ) {
            $limit_clause .= " OFFSET $first";
        }
    }

   $$statementref .= $limit_clause; 

}

# {{{ _MakeClauseCaseInsensitive

=head2 _MakeClauseCaseInsensitive FIELD OPERATOR VALUE

Takes a field, operator and value. performs the magic necessary to make
your database treat this clause as case insensitive.

Returns a FIELD OPERATOR VALUE triple.

=cut

sub _MakeClauseCaseInsensitive {
    my $self     = shift;
    my $field    = shift;
    my $operator = shift;
    my $value    = shift;


    if ($value =~ /^\d+$/) { # we don't need to downcase numeric values
        	return ( $field, $operator, $value);
    }

    if ( $operator =~ /LIKE/i ) {
        $operator =~ s/LIKE/ILIKE/ig;
        return ( $field, $operator, $value );
    }
    elsif ( $operator =~ /=/ ) {
	if (want(4)) {
        	return ( "LOWER($field)", $operator, $value, "LOWER(?)"); 
	} 
	# RT 3.0.x and earlier  don't know how to cope with a "LOWER" function 
	# on the value. they only expect field, operator, value.
	# 
	else {
		return ( "LOWER($field)", $operator, lc($value));

	}
    }
    else {
        $self->SUPER::_MakeClauseCaseInsensitive( $field, $operator, $value );
    }
}

# }}}
1;

__END__

=head1 SEE ALSO

DBIx::SearchBuilder, DBIx::SearchBuilder::Handle

=cut

