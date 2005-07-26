package Jifty::DBI::Handle::mysql;
use Jifty::DBI::Handle;
@ISA = qw(Jifty::DBI::Handle);

use vars qw($VERSION @ISA $DBIHandle $DEBUG);
use strict;

=head1 NAME

  Jifty::DBI::Handle::mysql - A mysql specific Handle object

=head1 SYNOPSIS


=head1 DESCRIPTION

This module provides a subclass of L<Jifty::DBI::Handle> that
compensates for some of the idiosyncrasies of MySQL.

=head1 METHODS

=cut

=head2 insert

Takes a table name as the first argument and assumes that the rest of
the arguments are an array of key-value pairs to be inserted.

If the insert succeeds, returns the id of the insert, otherwise,
returns a L<Class::ReturnValue> object with the error reported.

=cut

sub insert {
    my $self = shift;

    my $sth = $self->SUPER::insert(@_);
    if ( !$sth ) {
        return ($sth);
    }

    $self->{'id'} = $self->dbh->{'mysql_insertid'};

    # Yay. we get to work around mysql_insertid being null some of the time :/
    unless ( $self->{'id'} ) {
        $self->{'id'} = $self->fetch_result('SELECT LAST_INSERT_ID()');
    }
    warn "$self no row id returned on row creation" unless ( $self->{'id'} );

    return ( $self->{'id'} );    #Add Succeded. return the id
}

=head2 database_version

Returns the mysql version, trimming off any -foo identifier

=cut

sub database_version {
    my $self = shift;
    my $v    = $self->SUPER::database_version();

    $v =~ s/\-.*$//;
    return ($v);
}

=head2 case_sensitive 

Returns undef, since mysql's searches are not case sensitive by default 

=cut

sub case_sensitive {
    my $self = shift;
    return (undef);
}

1;

__END__

=head1 AUTHOR

Jesse Vincent, jesse@fsck.com

=head1 SEE ALSO

L<Jifty::DBI>, L<Jifty::DBI::Handle>, L<DBD::mysql>

=cut

