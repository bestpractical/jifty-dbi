package DBIx::SearchBuilder::Union;
use strict;
use warnings;

# WARNING --- This is still development code.  It is experimental.

our $VERSION = '$Version$';

# This could inherit from DBIx::SearchBuilder, but there are _a lot_
# of things in DBIx::SearchBuilder that we don't want, like Limit and
# stuff.  It probably makes sense to (eventually) split out
# DBIx::SearchBuilder::Collection to contain all the iterator logic.
# This could inherit from that.


=head1 NAME

DBIx::Searchbuilder::Union - Deal with multiple SearchBuilder result sets as one

=head1 SYNOPSIS

  use DBIx::SearchBuilder::Union;
  my $U = new DBIx::SearchBuilder::Union;
  $U->add( $tickets1 );
  $U->add( $tickets2 );

=head1 DESCRIPTION

Implements a subset of the DBIx::SearchBuilder collection methods, but
enough to do iteration over a bunch of results.  Useful for displaying
the results of two unrelated searches (for the same kind of objects)
in a single list.

=head1 AUTHOR

  Robert Spier

=cut

=head1 METHODS

=head2 new

Create a new DBIx::SearchBuilder::Union object.  No arguments.

=cut

sub new {
  bless {
		 data => [],
		 curp => 0,				# current offset in data
		 item => 0,				# number of indiv items from First
		 count => undef,
		}, shift;
}

# private
sub curp {
   shift->{curp}
}

=head2 add $sb

Add a searchbuilder result (collection) to the Union object.

It must be the same type as the first object added.

=cut

sub add {
    my $self   = shift;
	my $newobj = shift;

	unless ( @{$self->{data}} == 0
			 || ref($newobj) eq ref($self->{data}[0]) ) {
	  die "All elements of a DBIx::SearchBuilder::Union must be of the same type.  Looking for a " . ref($self->{data}[0]) .".";
	}

	$self->{count} = undef;
    push @{$self->{data}}, $newobj;
}

=head2 First

Return the very first element of the Union (which is the first element
of the first Collection).  Also reset the current pointer to that
element.

=cut

sub First {
    my $self = shift;

	die "No elements in DBIx::SearchBuilder::Union"
	  unless @{$self->{data}};

    $self->{curp} = 0;
	$self->{item} = 0;
    $self->{data}[0]->First;
}

=head2 Next

Return the next element in the Union.

=cut

sub Next {
  my $self=shift;

  return undef unless defined  $self->{data}[ $self->curp ];

  my $cur =  $self->{data}[ $self->curp ];
  if ( $cur->_ItemsCounter == $cur->Count ) {
	# move to the next element
	$self->{curp}++;
	return undef unless defined   $self->{data}[ $self->curp ];
	$cur =  $self->{data}[ $self->curp ];
	$self->{data}[ $self->{curp} ]->GotoFirstItem;
  }
  $self->{item}++;
  $cur->Next;
}

=head2 Last

Returns the last item

=cut

sub Last {
  die "Last doesn't work right now";
  my $self = shift;
  $self->GotoItem( ( $self->Count ) - 1 );
  return ( $self->Next );
}

sub Count {
  my $self = shift;
  my $sum = 0;

  return $self->{count} if defined $self->{count};

  $sum += $_->Count for (@{$self->{data}});

  $self->{count} = $sum;

  return $sum;
}


=head2 GotoFirstItem

Starts the recordset counter over from the first item. the next time
you call Next, you'll get the first item returned by the database, as
if you'd just started iterating through the result set.

=cut

sub GotoFirstItem {
  my $self = shift;
  $self->GotoItem(0);
}

sub GotoItem {
  my $self = shift;
  my $item = shift;

  die "We currently only support going to the First item"
	unless $item == 0;

  $self->{curp} = 0;
  $self->{item} = 0;
  $self->{data}[0]->GotoItem(0);

  return $item;
}

=head2 IsLast

Returns true if the current row is the last record in the set.

=cut

sub IsLast {
    my $self = shift;

	$self->{item} == $self->Count ? 1 : undef;
}

=head2 ItemsArrayRef

Return a refernece to an array containing all objects found by this search.

Will destroy any positional state.

=cut

sub ItemsArrayRef {
    my $self = shift;

    return [] unless $self->Count;

	$self->GotoFirstItem();
	my @ret;
	while( my $r = $self->Next ) {
	  push @ret, $r;
	}

	return \@ret;
}




1;

__END__

