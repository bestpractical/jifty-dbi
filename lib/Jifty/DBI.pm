package Jifty::DBI;
use warnings;
use strict;

$Jifty::DBI::VERSION = '0.42';

=head1 NAME

Jifty::DBI - An object-relational persistence framework

=head1 DESCRIPTION

Jifty::DBI deals with databases, so that you don't have to.

This module provides an object-oriented mechanism for retrieving and
updating data in a DBI-accessible database. 

This module is the direct descendent of L<DBIx::SearchBuilder>. If you're familiar
with SearchBuilder, Jifty::DBI should be quite familiar to you.

=head2 What is it trying to do. 

Jifty::DBI::Record abstracts the agony of writing the common and generally 
simple SQL statements needed to serialize and de-serialize an object to the
database.  In a traditional system, you would define various methods on 
your object 'create', 'read', 'update', and 'delete' being the most common. 
In each method you would have a SQL statement like: 

  select * from table where value='blah';

If you wanted to control what data a user could modify, you would have to 
do some special magic to make accessors do the right thing. Etc.  The 
problem with this approach is that in a majority of the cases, the SQL is 
incredibly simple and the code from one method/object to the next was 
basically the same.  

<trumpets>

Enter, Jifty::DBI::Record. 

With ::Record, you can in the simple case, remove all of that code and 
replace it by defining two methods and inheriting some code.  It's pretty 
simple and incredibly powerful.  For more complex cases, you can 
do more complicated things by overriding certain methods.  Let's stick with
the simple case for now. 



=head2 An Annotated Example

The example code below makes the following assumptions: 

=over 4

=item *

The database is 'postgres',

=item *

The host is 'reason',

=item *

The login name is 'mhat',

=item *

The database is called 'example', 

=item *

The table is called 'simple', 

=item *

The table looks like so: 

      id     integer     not NULL,   primary_key(id),
      foo    varchar(10),
      bar    varchar(10)

=back

First, let's define our record class in a new module named "Simple.pm".

  use warnings;
  use strict;

  package Simple;
  use Jifty::DBI::Schema;
  use Jifty::DBI::Record schema {
    column foo => type is 'text';
    column bar => type is 'text';
  };

  # your custom code goes here.

  1;

Like all perl modules, this needs to end with a true value. 

Now, on to the code that will actually *do* something with this object. 
This code would be placed in your Perl script.

  use Jifty::DBI::Handle;
  use Simple;

Use two packages, the first is where I get the DB handle from, the latter 
is the object I just created. 


  my $handle = Jifty::DBI::Handle->new();
  $handle->connect(
      driver   => 'Pg',
      database => 'test',
      host     => 'reason',
      user     => 'mhat',
      password => ''
  );

Creates a new Jifty::DBI::Handle, and then connects to the database using 
that handle.  Pretty straight forward, the password '' is what I use 
when there is no password.  I could probably leave it blank, but I find 
it to be more clear to define it.


 my $s = Simple->new( handle => $handle );

 $s->load_by_cols(id=>1); 


=over

=item load_by_cols

Takes a hash of column => value pairs and returns the *first* to match. 
First is probably lossy across databases vendors. 

=item load_from_hash

Populates this record with data from a Jifty::DBI::Collection.  I'm 
currently assuming that Jifty::DBI is what we use in 
cases where we expect > 1 record.  More on this later.

=back

Now that we have a populated object, we should do something with it! ::Record
automagically generates accessors and mutators for us, so all we need to do 
is call the methods.  accessors are named C<column>(), and Mutators are named 
C<set_column>($).  On to the example, just appending this to the code from 
the last example.


 print "ID  : ", $s->id(),  "\n";
 print "Foo : ", $s->foo(), "\n";
 print "Bar : ", $s->bar(), "\n";

Thats all you have to to get the data, now to change the data!


 $s->set_bar('NewBar');

Pretty simple! Thats really all there is to it.  Set<Field>($) returns 
a boolean and a string describing the problem.  Lets look at an example of
what will happen if we try to set a 'Id' which we previously defined as 
read only. 

 my ($res, $str) = $s->set_id('2');
 if (! $res) {
   ## Print the error!
   print "$str\n";
 } 

The output will be:

  >> Immutable column

Currently Set<Field> updates the data in the database as soon as you call
it.  In the future I hope to extend ::Record to better support transactional
operations, such that updates will only happen when "you" say so.

Finally, adding and removing records from the database.  ::Record provides a 
Create method which simply takes a hash of key => value pairs.  The keys 
exactly map to database columns. 

 ## Get a new record object.
 $s1 = Simple->new( handle => $handle );
 my ($id, $status_msg) = $s1->create(id  => 4,
                   foo => 'Foooooo', 
                   bar => 'Barrrrr');

Poof! A new row in the database has been created!  Now lets delete the 
object! 

 my $s2 = Simple->new( handle => $handle );
 $s2->load_by_cols(id=>4);
 $s2->delete();

And it's gone. 

For simple use, thats more or less all there is to it.  In the future, I hope to exapand 
this HowTo to discuss using container classes,  overloading, and what 
ever else I think of.

=head1 LICENSE

Jifty::DBI is Copyright 2005-2007 Best Practical Solutions, LLC.
Jifty::DBI is distributed under the same terms as Perl itself.

=cut

1;
