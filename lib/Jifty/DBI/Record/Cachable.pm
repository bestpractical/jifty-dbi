package Jifty::DBI::Record::Cachable;

use Jifty::DBI::Record;
use Jifty::DBI::Handle;
@ISA = qw (Jifty::DBI::Record);

use Cache::Simple::TimedExpiry;

use strict;

=head1 NAME

Jifty::DBI::Record::Cachable - records with caching behavior

=head1 SYNOPSIS

  package Myrecord;
  use base qw/Jifty::DBI::Record::Cachable/;

=head1 DESCRIPTION

This module subclasses the main L<Jifty::DBI::Record> package to add a
caching layer.

The public interface remains the same, except that records which have
been loaded in the last few seconds may be reused by subsequent fetch
or load methods without retrieving them from the database.

=head1 METHODS

=cut

my %_CACHES = ();

# Function: new
# Type    : class ctor
# Args    : see Jifty::DBI::Record::new
# Lvalue  : Jifty::DBI::Record::Cachable

sub new () {
    my ( $class, @args ) = @_;
    my $self = $class->SUPER::new(@args);

    return ($self);
}

sub _setup_cache {
    my $self  = shift;
    my $cache = shift;
    $_CACHES{$cache} = Cache::Simple::TimedExpiry->new();
    $_CACHES{$cache}->expire_after( $self->_cache_config->{'cache_for_sec'} );
}

=head2 flush_cache 

This class method flushes the _global_ Jifty::DBI::Record::Cachable 
cache.  All caches are immediately expired.

=cut

sub flush_cache {
    %_CACHES = ();
}

sub _key_cache {
    my $self  = shift;
    my $cache = $self->_handle->DSN
        . "-KEYS--"
        . ( $self->{'_class'} ||= ref($self) );
    $self->_setup_cache($cache) unless exists( $_CACHES{$cache} );
    return ( $_CACHES{$cache} );

}

=head2 _flush_key_cache

Blow away this record type's key cache

=cut

sub _flush_key_cache {
    my $self  = shift;
    my $cache = $self->_handle->DSN
        . "-KEYS--"
        . ( $self->{'_class'} ||= ref($self) );
    $self->_setup_cache($cache);
}

sub _record_cache {
    my $self = shift;
    my $cache
        = $self->_handle->DSN . "--" . ( $self->{'_class'} ||= ref($self) );
    $self->_setup_cache($cache) unless exists( $_CACHES{$cache} );
    return ( $_CACHES{$cache} );

}

sub load_from_hash {
    my $self = shift;

    # Blow away the primary cache key since we're loading.
    $self->{'_jifty_cache_pkey'} = undef;
    my ( $rvalue, $msg ) = $self->SUPER::load_from_hash(@_);

    my $cache_key = $self->_primary_record_cache_key();

    ## Check the return value, if its good, cache it!
    if ($rvalue) {
        $self->_store();
    }

    return ( $rvalue, $msg );
}

sub load_by_cols {
    my ( $self, %attr ) = @_;
    ## Generate the cache key
    my $alt_key = $self->_gen_alternate_record_cache_key(%attr);
    if ( $self->_fetch( $self->_lookup_primary_record_cache_key($alt_key) ) ) {
        return ( 1, "Fetched from cache" );
    }
    #warn "Didn't get it from the cache";
    # Blow away the primary cache key since we're loading.
    $self->{'_jifty_cache_pkey'} = undef;

    ## Fetch from the DB!
    my ( $rvalue, $msg ) = $self->SUPER::load_by_cols(%attr);
    ## Check the return value, if its good, cache it!
    if ($rvalue) {
        ## Only cache the object if its okay to do so.
        $self->_store();
        $self->_key_cache->set( $alt_key, $self->_primary_record_cache_key );

    }
    return ( $rvalue, $msg );

}

# Function: __Set
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
    $self->_record_cache->set( $self->_primary_record_cache_key, undef, time - 1 );

    # We should be doing something more surgical to clean out the key cache. but we do need to expire it
    $self->_flush_key_cache;

}

# Function: _fetch
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Get an object from the cache, and make this object that.

sub _fetch () {
    my ( $self, $cache_key ) = @_;
    my $data = $self->_record_cache->fetch($cache_key) or return;

    @{$self}{ keys %$data } = values %$data;    # deserialize
    return 1;

}

#sub __value {
#    my $self   = shift;
#    my $column = shift;
#
#    # XXX TODO, should we be fetching directly from the cache?
#    return ( $self->SUPER::__value($column) );
#}

# Function: _store
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Stores this object in the cache.

sub _store (\$) {
    my $self = shift;
    $self->_record_cache->set( $self->_primary_record_cache_key,
        {   values  => $self->{'values'},
            table   => $self->table,
            fetched => $self->{'fetched'}
        }
    );
}


# Function: _gen_alternate_record_cache_key
# Type    : private instance
# Args    : hash (attr)
# Lvalue  : 1
# Desc    : Takes a perl hash and generates a key from it.

sub _gen_alternate_record_cache_key {
    my ( $self, %attr ) = @_;

    my $cache_key;
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

# Function: _fetch_record_cache_key
# Type    : private instance
# Args    : nil
# Lvalue  : 1

sub _fetch_record_cache_key {
    my ($self) = @_;
    my $cache_key = $self->_cache_config->{'cache_key'};
    return ($cache_key);
}

# Function: _primary_record_cache_key
# Type    : private instance
# Args    : none
# Lvalue: : 1
# Desc    : generate a primary-key based variant of this object's cache key
#           primary keys is in the cache

sub _primary_record_cache_key {
    my ($self) = @_;

    return unless ( defined $self->id );

    unless ( $self->{'_jifty_cache_pkey'} ) {

        my $primary_record_cache_key = $self->table() . ':';
        my @attributes;
        foreach my $key ( @{ $self->_primary_keys } ) {
            push @attributes, $key . '=' . $self->SUPER::__value($key);
        }

        $primary_record_cache_key .= join( ',', @attributes );

        $self->{'_jifty_cache_pkey'}
            = $primary_record_cache_key;
    }
    return ( $self->{'_jifty_cache_pkey'} );

}

# Function: lookup_primary_record_cache_key
# Type    : private class
# Args    : string(alternate cache id)
# Lvalue  : string(cache id)
sub _lookup_primary_record_cache_key {
    my $self          = shift;
    my $alternate_key = shift;
    return undef unless ($alternate_key);

    my $primary_key = $self->_key_cache->fetch($alternate_key);
    if ($primary_key) {
        return ($primary_key);
    }

    # If the alternate key is really the primary one
    elsif ( $self->_record_cache->fetch($alternate_key) ) {
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
    {   'cache_p'       => 1,
        'cache_for_sec' => 5,
    };
}

1;

__END__


=head1 AUTHOR

Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

L<Jifty::DBI>, L<Jifty::DBI::Record>

=cut


