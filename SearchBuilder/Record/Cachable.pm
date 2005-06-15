# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Record/Cachable.pm,v 1.6 2001/06/19 04:22:32 jesse Exp $
# by Matt Knopp <mhat@netlag.com>

package DBIx::SearchBuilder::Record::Cachable;

use DBIx::SearchBuilder::Record;
use DBIx::SearchBuilder::Handle;
@ISA = qw (DBIx::SearchBuilder::Record);

use Cache::Simple::TimedExpiry;

use strict;


=head1 NAME

DBIx::SearchBuilder::Record::Cachable - Records with caching behavior

=head1 SYNOPSIS

  package MyRecord;
  use base qw/DBIx::SearchBuilder::Record::Cachable/;

=head1 DESCRIPTION

This module subclasses the main DBIx::SearchBuilder::Record package to add a caching layer. 

The public interface remains the same, except that records which have been loaded in the last few seconds may be reused by subsequent fetch or load methods without retrieving them from the database.

=head1 METHODS

=cut


my %_CACHES = ();

# Function: new
# Type    : class ctor
# Args    : see DBIx::SearchBuilder::Record::new
# Lvalue  : DBIx::SearchBuilder::Record::Cachable

sub new () {
    my ( $class, @args ) = @_;
    my $self = $class->SUPER::new(@args);

    return ($self);
}

sub _SetupCache {
    my $self  = shift;
    my $cache = shift;
    $_CACHES{$cache} = Cache::Simple::TimedExpiry->new();
    $_CACHES{$cache}->expire_after( $self->_CacheConfig->{'cache_for_sec'} );
}

=head2 FlushCache 

This class method flushes the _global_ DBIx::SearchBuilder::Record::Cachable 
cache.  All caches are immediately expired.

=cut

sub FlushCache {
    %_CACHES = ();
}


sub _KeyCache {
    my $self = shift;
    my $cache = $self->_Handle->DSN . "-KEYS--" . ($self->{'_Class'} ||= ref($self));
    $self->_SetupCache($cache) unless exists ($_CACHES{$cache});
    return ($_CACHES{$cache});

}

=head2 _FlushKeyCache

Blow away this record type's key cache

=cut


sub _FlushKeyCache {
    my $self = shift;
    my $cache = $self->_Handle->DSN . "-KEYS--" . ($self->{'_Class'} ||= ref($self));
    $self->_SetupCache($cache);
}

sub _RecordCache {
    my $self = shift;
    my $cache = $self->_Handle->DSN . "--" . ($self->{'_Class'} ||= ref($self));
    $self->_SetupCache($cache) unless exists ($_CACHES{$cache});
    return ($_CACHES{$cache});

}

# Function: LoadFromHash
# Type    : (overloaded) public instance
# Args    : See DBIx::SearchBuilder::Record::LoadFromHash
# Lvalue  : array(boolean, message)

sub LoadFromHash {
    my $self = shift;

    # Blow away the primary cache key since we're loading.
    $self->{'_SB_Record_Primary_RecordCache_key'} = undef;
    my ( $rvalue, $msg ) = $self->SUPER::LoadFromHash(@_);

    my $cache_key = $self->_primary_RecordCache_key();

    ## Check the return value, if its good, cache it!
    if ($rvalue) {
        $self->_store();
    }

    return ( $rvalue, $msg );
}

# Function: LoadByCols
# Type    : (overloaded) public instance
# Args    : see DBIx::SearchBuilder::Record::LoadByCols
# Lvalue  : array(boolean, message)

sub LoadByCols {
    my ( $self, %attr ) = @_;

    ## Generate the cache key
    my $alt_key = $self->_gen_alternate_RecordCache_key(%attr);
    if ( $self->_fetch( $self->_lookup_primary_RecordCache_key($alt_key) ) ) {
        return ( 1, "Fetched from cache" );
    }

    # Blow away the primary cache key since we're loading.
    $self->{'_SB_Record_Primary_RecordCache_key'} = undef;

    ## Fetch from the DB!
    my ( $rvalue, $msg ) = $self->SUPER::LoadByCols(%attr);
    ## Check the return value, if its good, cache it!
    if ($rvalue) {
        ## Only cache the object if its okay to do so.
        $self->_store();
        $self->_KeyCache->set( $alt_key, $self->_primary_RecordCache_key);

    }
    return ( $rvalue, $msg );

}

# Function: __Set
# Type    : (overloaded) public instance
# Args    : see DBIx::SearchBuilder::Record::_Set
# Lvalue  : ?

sub __Set () {
    my ( $self, %attr ) = @_;

    $self->_expire();
    return $self->SUPER::__Set(%attr);

}

# Function: Delete
# Type    : (overloaded) public instance
# Args    : nil
# Lvalue  : ?

sub __Delete () {
    my ($self) = @_;

    $self->_expire();

    return $self->SUPER::__Delete();

}

# Function: _expire
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Removes this object from the cache.

sub _expire (\$) {
    my $self = shift;
    $self->_RecordCache->set( $self->_primary_RecordCache_key , undef, time-1);
    # We should be doing something more surgical to clean out the key cache. but we do need to expire it
    $self->_FlushKeyCache;
   
}

# Function: _fetch
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Get an object from the cache, and make this object that.

sub _fetch () {
    my ( $self, $cache_key ) = @_;
    my $data = $self->_RecordCache->fetch($cache_key) or return;

    @{$self}{keys %$data} = values %$data; # deserialize
    return 1;

}


sub __Value {
    my $self  = shift;
    my $field = shift;
    return ( $self->SUPER::__Value($field) );
}

# Function: _store
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Stores this object in the cache.

sub _store (\$) {
    my $self = shift;
    $self->_RecordCache->set( $self->_primary_RecordCache_key, $self->_serialize);
    return (1);
}

sub _serialize {
    my $self = shift;
    return (
        {
            values  => $self->{'values'},
            table   => $self->Table,
            fetched => $self->{'fetched'}
        }
    );
}

# Function: _gen_alternate_RecordCache_key
# Type    : private instance
# Args    : hash (attr)
# Lvalue  : 1
# Desc    : Takes a perl hash and generates a key from it.

sub _gen_alternate_RecordCache_key {
    my ( $self, %attr ) = @_;
    #return( Storable::nfreeze( %attr));
   my $cache_key;
    while ( my ( $key, $value ) = each %attr ) {
        $key   ||= '__undef';
        $value ||= '__undef';

        if ( ref($value) eq "HASH" ) {
            $value = ( $value->{operator} || '=' ) . $value->{value};
        }
        else {
            $value = "=" . $value;
        }
        $cache_key .= $key . $value . ',';
    }
    chop($cache_key);
    return ($cache_key);
}

# Function: _fetch_RecordCache_key
# Type    : private instance
# Args    : nil
# Lvalue  : 1

sub _fetch_RecordCache_key {
    my ($self) = @_;
    my $cache_key = $self->_CacheConfig->{'cache_key'};
    return ($cache_key);
}

# Function: _primary_RecordCache_key
# Type    : private instance
# Args    : none
# Lvalue: : 1
# Desc    : generate a primary-key based variant of this object's cache key
#           primary keys is in the cache

sub _primary_RecordCache_key {
    my ($self) = @_;

    return undef unless ( $self->Id );

    unless ( $self->{'_SB_Record_Primary_RecordCache_key'} ) {

        my $primary_RecordCache_key = $self->Table() . ':';
        my @attributes;
        foreach my $key ( @{ $self->_PrimaryKeys } ) {
            push @attributes, $key . '=' . $self->SUPER::__Value($key);
        }

        $primary_RecordCache_key .= join( ',', @attributes );

        $self->{'_SB_Record_Primary_RecordCache_key'} = $primary_RecordCache_key;
    }
    return ( $self->{'_SB_Record_Primary_RecordCache_key'} );

}

# Function: lookup_primary_RecordCache_key
# Type    : private class
# Args    : string(alternate cache id)
# Lvalue  : string(cache id)
sub _lookup_primary_RecordCache_key {
    my $self          = shift;
    my $alternate_key = shift;
    return undef unless ($alternate_key);

    my $primary_key   = $self->_KeyCache->fetch($alternate_key);
    if ($primary_key) {
        return ($primary_key);
    }

    # If the alternate key is really the primary one
    elsif ( $self->_RecordCache->fetch($alternate_key) ) {
        return ($alternate_key);
    }
    else {    # empty!
        return (undef);
    }

}

=head2 _CacheConfig 

You can override this method to change the duration of the caching from the default of 5 seconds. 

For example, to cache records for up to 30 seconds, add the following method to your class:

  sub _CacheConfig {
      { 'cache_for_sec' => 30 }
  }

=cut

sub _CacheConfig {
    {
        'cache_p'       => 1,
        'cache_for_sec' => 5,
    };
}

1;

__END__


=head1 AUTHOR

Matt Knopp <mhat@netlag.com>

=head1 SEE ALSO

L<DBIx::SearchBuilder>, L<DBIx::SearchBuilder::Record>

=cut


