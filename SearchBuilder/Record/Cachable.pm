# $Header: /home/jesse/DBIx-SearchBuilder/history/SearchBuilder/Record/Cachable.pm,v 1.6 2001/06/19 04:22:32 jesse Exp $
# by Matt Knopp <mhat@netlag.com>

package DBIx::SearchBuilder::Record::Cachable; 

use DBIx::SearchBuilder::Record; 
use DBIx::SearchBuilder::Handle;
@ISA = qw (DBIx::SearchBuilder::Record);

my %_RECORD_CACHE = (); 
my %_KEY_CACHE = (); 


# Function: new 
# Type    : class ctor
# Args    : see DBIx::SearchBuilder::Record::new
# Lvalue  : DBIx::SearchBuilder::Record::Cachable

sub new () { 
  my ($class, @args) = @_; 
  my $this = $class->SUPER::new (@args);
 
  if ($this->can(_CacheConfig)) { 
     $this->{'_CacheConfig'}=$this->_CacheConfig();
  }
  else {
     $this->{'_CacheConfig'}=__CachableDefaults::_CacheConfig();
  }

  return ($this);
}



# Function: _RecordCache
# Type    : private instance
# Args    : none
# Lvalue  : hash: RecordCache
# Desc    : Returns a reference to the record cache hash

sub _RecordCache {
    my $this = shift;
    return(\%_RECORD_CACHE);
}

# Function: _KeyCache
# Type    : private instance
# Args    : none
# Lvalue  : hash: KeyCache
# Desc    : Returns a reference to the Key cache hash

sub _KeyCache {
    my $this = shift;
    return(\%_KEY_CACHE);
}



# Function: LoadFromHash
# Type    : (overloaded) public instance
# Args    : See DBIx::SearchBuilder::Record::LoadFromHash
# Lvalue  : array(boolean, message)

sub LoadFromHash {
    my $this = shift;
    my ($rvalue, $msg) = $this->SUPER::LoadFromHash(@_);

    my $cache_key = $this->_gen_primary_cache_key();


    ## Check the return value, if its good, cache it! 
    if ($rvalue) {
     ## Only cache the object if its okay to do so. 
    if ($this->{'_CacheConfig'}{'cache_p'}) {
        $this->_store() ;
    }
    }

    return($rvalue,$msg);
}

# Function: LoadByCols
# Type    : (overloaded) public instance
# Args    : see DBIx::SearchBuilder::Record::LoadByCols
# Lvalue  : array(boolean, message)

sub LoadByCols { 
  my ($this, %attr) = @_; 

  ## Generate the cache key
  my $alternate_key=$this->_gen_alternate_cache_key(%attr);
  my $cache_key = $this->_lookup_primary_cache_key($alternate_key);

  if ($cache_key && exists $this->_RecordCache->{$cache_key}) { 
    # We should never be caching a record without storing the time
    $cache_time =( $this->_RecordCache->{$cache_key}{'time'} || 0);

    ## Decide if the cache object is too old

    if ((time() - $cache_time) <= $this->{'_CacheConfig'}{'cache_for_sec'}) {
	    $this->_fetch($cache_key); 
	    return (1, "Fetched from cache");
    }
    else { 
      $this->_gc_expired();
    }
  } 

  ## Fetch from the DB!
  my ($rvalue, $msg) = $this->SUPER::LoadByCols(%attr);
  ## Check the return value, if its good, cache it! 
  if ($rvalue) {
    ## Only cache the object if its okay to do so. 
    $this->_store() if ($this->{'_CacheConfig'}{'cache_p'});

    my $new_cache_key = $this->_gen_primary_cache_key();
    $this->_KeyCache->{$alternate_key} = $new_cache_key;
    $this->_KeyCache->{$alternate_key}{'time'} = time();
  } 
  return ($rvalue, $msg);

}


# Function: _Set
# Type    : (overloaded) public instance
# Args    : see DBIx::SearchBuilder::Record::_Set
# Lvalue  : ?

sub __Set () { 
  my ($this, %attr) = @_; 

  $this->_expire( $this->_gen_primary_cache_key());
 
  return $this->SUPER::__Set(%attr);

}


# Function: Delete
# Type    : (overloaded) public instance
# Args    : nil
# Lvalue  : ?

sub Delete () { 
  my ($this) = @_; 
  my $cache_key = $this->_gen_primary_cache_key();

  $this->_expire($cache_key);
 
  return $this->SUPER::Delete();

}





# Function: _gc_expired
# Type    : private instance
# Args    : nil
# Lvalue  : 1
# Desc    : Looks at all cached objects and expires if needed. 

sub _gc_expired () { 
  my ($this) = @_; 


  my $time = time();  

  # XXX TODO: do we want to sort the keys beforehand, so we can get out of the loop earlier?
  foreach my $cache_key (keys %{$this->_KeyCache}, keys %{$this->_RecordCache}) {
    my $cache_time = $this->_RecordCache->{$cache_key}{'time'} || 0 ;  
    $this->_expire($cache_key) 
      if (($time - $cache_time) > $this->{'_CacheConfig'}{'cache_for_sec'});
  }
}




# Function: _expire
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Removes this object from the cache. 

sub _expire (\$) {
  my ($this, $cache_key) = @_; 
  delete $this->_RecordCache->{$cache_key} if (exists $this->_RecordCache->{$cache_key});
  return (1);
}




# Function: _fetch
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Get an object from the cache, and make this object that. 

sub _fetch () { 
  my ($this, $cache_key) = @_;

  $this->{'values'}  = $this->_RecordCache->{$cache_key}{'values'};
  $this->{'fetched'}  = $this->_RecordCache->{$cache_key}{'fetched'};
  return(1); 
}

sub __Value {
 my $self = shift;
  my $field = shift;

    $field = lc $field;
    my $cache_key = $self->_gen_primary_cache_key();
    unless ( $cache_key
           && exists $self->_RecordCache->{$cache_key}{'values'}->{"$field"} ) {
           return($self->SUPER::__Value($field));
    }
   return($self->_RecordCache->{$cache_key}{'values'}->{"$field"});


}



# Function: _store
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Stores this object in the cache. 

sub _store (\$) { 
  my ($this) = @_; 
  my $cache_key = $this->_gen_primary_cache_key();
  $this->{'_CacheConfig'}{'cache_key'} = $cache_key;
  $this->_RecordCache->{$cache_key}{'values'} = $this->{'values'};
  $this->_RecordCache->{$cache_key}{'fetched'} = $this->{'fetched'};
  $this->_RecordCache->{$cache_key}{'time'} = time();
  
  return(1);
}




# Function: _gen_alternate_cache_key
# Type    : private instance
# Args    : hash (attr)
# Lvalue  : 1
# Desc    : Takes a perl hash and generates a key from it. 

sub _gen_alternate_cache_key {
    my ( $this, %attr ) = @_;
    my $cache_key = $this->Table() . ':';
    while ( my ( $key, $value ) = each %attr ) {
        $key ||= '__undef';
        $value ||= '__undef';

        if ( ref($value) eq "HASH" ) { 
            $value = $value->{operator}.$value->{value}; 
        } else {
            $value = "=".$value;
        }    
        $cache_key .= $key.$value.',';
    }
    chop($cache_key);
    return ($cache_key);
}


# Function: _fetch_cache_key
# Type    : private instance
# Args    : nil
# Lvalue  : 1

sub _fetch_cache_key {
    my ($this) = @_;
    my $cache_key = $this->{'_CacheConfig'}{'cache_key'};
    return($cache_key);
}



# Function: _gen_primary_cache_key 
# Type    : private instance
# Args    : none
# Lvalue: : 1
# Desc    : generate a primary-key based variant of this object's cache key
#           primary keys is in the cache 

sub _gen_primary_cache_key {
    my ($this) = @_;


    return undef unless ($this->Id);

    my $primary_cache_key = $this->Table() . ':';
    my @attributes; 
    foreach my $key (@{$this->_PrimaryKeys}) {
        push @attributes, $key.'='.  $this->SUPER::__Value($key);
    }

    $primary_cache_key .= join(',',@attributes);

    return($primary_cache_key);

}


# Function: lookup_primary_cache_key 
# Type    : private class
# Args    : string(alternate cache id)
# Lvalue  : string(cache id)
sub _lookup_primary_cache_key {
    my $this          = shift;
    my $alternate_key = shift;  
    if ( exists $this->_KeyCache->{$alternate_key} ) {
        $cache_time = $this->_KeyCache->{$alternate_key}{'time'};

        ## Decide if the cache object is too old
        if ( ( time() - $cache_time ) <=
             $this->{'_CacheConfig'}{'cache_for_sec'} ) {
            return $this->_KeyCache->{$alternate_key};
        }
        else {
            $this->_gc_expired();
        }
    }
    # If what we thought was the alternate key was actually the primary key
    if ($alternate_key && exists $this->_RecordCache->{$alternate_key}) { 
        return($alternate_key);
    }
    # not found
    return (undef);
}


package __CachableDefaults; 

sub _CacheConfig { 
  { 
     'cache_p'        => 1,
     'cache_for_sec'  => 5,
  }
}
1;
