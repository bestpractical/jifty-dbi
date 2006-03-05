use warnings;
use strict;

package Jifty::DBI::Record::Memcached;

use Jifty::DBI::Record;
use Jifty::DBI::Handle;
use base qw (Jifty::DBI::Record);

use Cache::Memcached;


=head1 NAME

Jifty::DBI::Record::Cachable - records with caching behavior

=head1 SYNOPSIS

  package Myrecord;
  use base qw/Jifty::DBI::Record::Cachable/;

=head1 DESCRIPTION

This module subclasses the main L<Jifty::DBI::Record> package to add a
caching layer.

The public interface remains the same, except that records which have
been loaded in the last few seconds may be reused by subsequent get
or load methods without retrieving them from the database.

=head1 METHODS

=cut


use vars qw/$MEMCACHED/;




# Function: new
# Type    : class ctor
# Args    : see Jifty::DBI::Record::new
# Lvalue  : Jifty::DBI::Record::Cachable

sub _init () {
    my ( $self, @args ) = @_;
    $MEMCACHED ||= $self->_setup_cache();
    $self->SUPER::_init(@_);
}

sub _setup_cache {
    my $self  = shift;
    my $cache = Cache::Memcached->new( {$self->memcached_config} );
    return $cache;
}

sub load_from_hash {
    my $self = shift;

    # Blow away the primary cache key since we're loading.
    $self->{'_jifty_cache_pkey'} = undef;
    my ( $rvalue, $msg ) = $self->SUPER::load_from_hash(@_);

    my $cache_key = $self->_primary_cache_key();

    ## Check the return value, if its good, cache it!
    if ($rvalue) {
        $self->_store();
    }

    return ( $rvalue, $msg );
}

sub load_by_cols {
    my ( $self, %attr ) = @_;

    ## Generate the cache key
    my $alt_key = $self->_gen_alternate_cache_key(%attr);
    if ( $self->_get($alt_key) 
            or  $self->_get( $self->_lookup_primary_cache_key($alt_key) ) ) {
        return ( 1, "Fetched from cache" );
    }

    # Blow away the primary cache key since we're loading.
    $self->{'_jifty_cache_pkey'} = undef;

    ## Fetch from the DB!
    my ( $rvalue, $msg ) = $self->SUPER::load_by_cols(%attr);
    ## Check the return value, if its good, cache it!
    if ($rvalue) {
        $self->_store();
        $MEMCACHED->set( $alt_key, $self->_primary_cache_key, $self->_cache_config->{'cache_for_sec'} );
        $self->{'loaded_by_cols'} = $alt_key;
    }
    return ( $rvalue, $msg );

}

# Function: __set
# Type    : (overloaded) public instance
# Args    : see Jifty::DBI::Record::_Set
# Lvalue  : ?

sub __set () {
    my ( $self, %attr ) = @_;
    $self->_expire();
    return $self->SUPER::__set(%attr);

}

# Function: Delete
# Type    : (overloaded) public instance
# Args    : nil
# Lvalue  : ?

sub __delete () {
    my ($self) = @_;
    $self->_expire();
    return $self->SUPER::__delete();
}

# Function: _expire
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Removes this object from the cache.

sub _expire (\$) {
    my $self = shift;
    $MEMCACHED->delete( $self->_primary_cache_key);
    $MEMCACHED->delete($self->{'loaded_by_cols'}) if ($self->{'loaded_by_cols'});

}

# Function: _get
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Get an object from the cache, and make this object that.

sub _get () {
    my ( $self, $cache_key ) = @_;
    my $data = $MEMCACHED->get($cache_key) or return;
    @{$self}{ keys %$data } = values %$data;    # deserialize
}

sub __value {
    my $self   = shift;
    my $column = shift;
    return ( $self->SUPER::__value($column) );
}

# Function: _store
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Stores this object in the cache.

sub _store (\$) {
    my $self = shift;
    $MEMCACHED->set( $self->_primary_cache_key,
        {   values  => $self->{'values'},
            table   => $self->table,
            geted => $self->{'fetched'}
        },
        $self->_cache_config->{'cache_for_sec'}
    );
}


# Function: _gen_alternate_cache_key
# Type    : private instance
# Args    : hash (attr)
# Lvalue  : 1
# Desc    : Takes a perl hash and generates a key from it.

sub _gen_alternate_cache_key {
    my ( $self, %attr ) = @_;

    my $cache_key = $self->table() . ':';
    while ( my ( $key, $value ) = each %attr ) {
        $key   ||= '__undef';
        $value ||= '__undef';

        if ( ref($value) eq "HASH" ) {
            $value = ( $value->{operator} || '=' ) . $value->{value};
        } else {
            $value = "=" . $value;
        }
        $cache_key .= $key . $value . ',';
    }
    chop($cache_key);
    return ($cache_key);
}

# Function: _get_cache_key
# Type    : private instance
# Args    : nil
# Lvalue  : 1

sub _get_cache_key {
    my ($self) = @_;
    my $cache_key = $$self->_cache_config->{'cache_key'};
    return ($cache_key);
}

# Function: _primary_cache_key
# Type    : private instance
# Args    : none
# Lvalue: : 1
# Desc    : generate a primary-key based variant of this object's cache key
#           primary keys is in the cache

sub _primary_cache_key {
    my ($self) = @_;

    return undef unless ( $self->id );

    unless ( $self->{'_jifty_cache_pkey'} ) {

        my $primary_cache_key = $self->table() . ':';
        my @attributes;
        foreach my $key ( @{ $self->_primary_keys } ) {
            push @attributes, $key . '=' . $self->SUPER::__value($key);
        }

        $primary_cache_key .= join( ',', @attributes );

        $self->{'_jifty_cache_pkey'} = $primary_cache_key;
    }
    return ( $self->{'_jifty_cache_pkey'} );

}

# Function: lookup_primary_cache_key
# Type    : private class
# Args    : string(alternate cache id)
# Lvalue  : string(cache id)
sub _lookup_primary_cache_key {
    my $self          = shift;
    my $alternate_key = shift;
    return undef unless ($alternate_key);

    my $primary_key = $MEMCACHED->get($alternate_key);
    if ($primary_key) {
        return ($primary_key);
    }

    # If the alternate key is really the primary one
    elsif ( $MEMCACHED->get($alternate_key) ) {
        return ($alternate_key);
    } else {    # empty!
        return (undef);
    }

}

=head2 _cache_config 

You can override this method to change the duration of the caching
from the default of 5 seconds.

For example, to cache records for up to 30 seconds, add the following
method to your class:

  sub _cache_config {
      { 'cache_for_sec' => 30 }
  }

=cut

sub _cache_config {
    {   
        'cache_for_sec' => 180,
    };
}


sub memcached_config {
    servers => ['127.0.0.1:11211'],
    debug => 0

}


1;

__END__


=head1 AUTHOR

Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

L<Jifty::DBI>, L<Jifty::DBI::Record>

=cut


