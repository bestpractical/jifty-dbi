# $Header: /raid/cvsroot/DBIx/DBIx-SearchBuilder/SearchBuilder/Record/Cachable.pm,v 1.1 2001/05/09 00:21:28 jesse Exp $
# DBIx::SearchBuilder::Record::Cachable by <mhat@netlag.com>

package DBIx::SearchBuilder::Record::Cachable; 

use DBIx::SearchBuilder::Record; 
use DBIx::SearchBuilder::Handle;
@ISA = qw (DBIx::SearchBuilder::Record);

my %_RECORD_CACHE = (); 



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

  $this->{'_RECORD_CACHE'}=\%_RECORD_CACHE;
  return ($this);
}




# Function: LoadByCols
# Type    : (overloaded) public instance
# Args    : see DBIx::SearchBuilder::Record::LoadByCols
# Lvalue  : array(boolean, message)

sub LoadByCols { 
  my ($this, %attr) = @_; 

  ## Generate the cache key
  my $cache_key=$this->_gen_cache_key(%attr);

  if (exists $this->{'_RECORD_CACHE'}{$cache_key}) { 
    $cache_time = $this->{'_RECORD_CACHE'}{$cache_key}{'time'};

    ## Decide if the cache object is too old
    if ((time() - $cache_time) <= $this->{'_CacheConfig'}{'cache_for_sec'}) {
      $this->_fetch($cache_key); 

      return (1, "Fetched from cache");
    }
    else { 
      $this->_expire($cache_key); 
    }
  } 

  ## Fetch from the DB!
  my ($rvalue, $msg) = $this->SUPER::LoadByCols(%attr);
 
  ## Only cache the object if its okay to do so. 
  $this->_store($cache_key) if ($this->{'_CacheConfig'}{'cache_p'});
  return ($rvalue, $msg);

  return (0, "Unexpected something or other [never hapens].");
}




# Function: _Set
# Type    : (overloaded) public instance
# Args    : see DBIx::SearchBuilder::Record::_Set
# Lvalue  : ?

sub _Set () { 
  my ($this, %attr) = @_; 
  my $cache_key = $this->{'_CacheConfig'}{'cache_key'};

  if (exists $this->{'_RECORD_CACHE'}{$cache_key}) {
    $this->_expire($cache_key);
  }
 
  return $this->SUPER::_Set(%attr);

}




# Function: _expire
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Removes this object from the cache. 

sub _expire () {
  my ($this, $cache_key) = @_; 
  delete $this->{'_RECORD_CACHE'}{$cache_key};
  return (1);
}




# Function: _fetch
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Get an object from the cache, and make this object that. 

sub _fetch () { 
  my ($this, $cache_key) = @_;

  $this->{'values'}  = 
    $this->{'_RECORD_CACHE'}{$cache_key}{'obj'}{'values'};

  return(1); 
}




# Function: _store
# Type    : private instance
# Args    : string(cache_key)
# Lvalue  : 1
# Desc    : Stores this object in the cache. 

sub _store () { 
  my ($this, $cache_key) = @_; 
  $this->{'_CacheConfig'}{'cache_key'} = $cache_key;
  $this->{'_RECORD_CACHE'}{$cache_key}{'obj'}=$this;
  $this->{'_RECORD_CACHE'}{$cache_key}{'time'}=time();
  
  return(1);
}




# Function: _gen_cache_key
# Type    : private instance
# Args    : hash (attr)
# Lvalue  : 1
# Desc    : Takes a perl hash and generates a key from it. 

sub _gen_cache_key {
  my ($this, %attr) = @_;
  my $cache_key="";
  while (my ($key, $value) = each %attr) {
    $cache_key .= $key . '=' . $value . ',';
  }
  chop ($cache_key);
  return ($cache_key);
}




package __CachableDefaults; 

sub _CacheConfig { 
  { 
     'cache_p'       => 1,
     'fast_update_p' => 1,
     'cache_for_sec' => 5,
  }
}

1;
