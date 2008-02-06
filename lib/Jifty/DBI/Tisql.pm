package Jifty::DBI::Tisql;

use strict;
use warnings;

use base qw(Parse::BooleanLogic);
use Scalar::Util qw(refaddr blessed);

use Regexp::Common qw(delimited);
my $re_delim  = qr{$RE{delimited}{-delim=>qq{\'\"}}};
my $re_field  = qr{[a-zA-Z][a-zA-Z0-9_]*};
my $re_column = qr{\.?$re_field(?:\.$re_field)*};
my $re_alias  = qr{$re_column\s+AS\s+$re_field}i;
my $re_sql_op_bin = qr{!?=|<>|>=?|<=?|(?:NOT )?LIKE}i;
my $re_sql_op_un  = qr{IS (?:NOT )?NULL}i;
my $re_value = qr{$re_delim|[0-9.]+};

my $re_op_positive = qr/^(?:=|IS|LIKE)$/i;
my $re_op_negative = qr/^(?:!=|<>|IS NOT|NOT LIKE)$/i;
my %invert_op = (
    '=' => '!=',
    '!=' => '=',
    '<>' => '=',
    'is' => 'IS NOT',
    'is not' => 'IS',
    'like' => 'NOT LIKE',
    'not like' => 'LIKE',
    '>' => '<=',
    '>=' => '<',
    '<' => '>=',
    '<=' => '>',
);

sub parse_query {
    my $self = shift;
    my $string = shift;

    my $tree = {
        joins => {},
        conditions => undef,
    };
    
    # parse "FROM..." prefix into $tree->{'joins'}
    if ( $string =~ s/^\s*FROM\s+($re_alias(?:\s*,\s*$re_alias)*)\s+WHERE\s+//oi ) {
        $tree->{'joins'}->{ $_->[1] } = $self->find_column( $_->[0], $tree->{'joins'} )
            foreach map [split /\s+AS\s+/i, $_], split /,/, $1;
    }

    $tree->{'conditions'} = $self->as_array(
        $string,
        operand_cb => sub { return $self->split_condition( $_[0], $tree->{'joins'} ) },
    );
    use Data::Dumper; warn Dumper( $tree->{'conditions'} );
    $self->apply_query_tree( $tree->{'conditions'} );
    return $tree;
}

sub apply_query_tree {
    my $self = shift;
    my $tree = shift;

    my $collection = $self->{'collection'};

    my $ea = shift || 'AND';
    $collection->open_paren('tisql');
    foreach my $element ( @$tree ) {
        unless ( ref $element ) {
            $ea = $element;
            next;
        }
        elsif ( ref $element eq 'ARRAY' ) {
            $self->apply_query_tree( $element, $ea );
            next;
        }
        elsif ( ref $element ne 'HASH' ) {
            die "wrong query tree";
        }

        my %limit = (
            subclause        => 'tisql',
            entry_aggregator => $ea,
            operator         => $element->{'op'},
        );
        if ( ref $element->{'lhs'} ) {
            my ($alias, $column) = $self->resolve_join( @{ $element->{'lhs'} } );
            @limit{qw(alias column)} = ($alias, $column->name);
        } else {
            die "left hand side must be always column specififcation";
        }
        if ( ref $element->{'rhs'} ) {
            my ($alias, $column) = $self->resolve_join( @{ $element->{'rhs'} } );
            @limit{qw(quote_value value)} = (0, $alias .'.'. $column->name );
        } else {
            $limit{'value'} = $element->{'rhs'};
        }

        $collection->limit( %limit );
    }
    $collection->close_paren('tisql');
}

sub resolve_join {
    my $self = shift;
    my @chain = @_;
    if ( @chain == 1 ) {
        return 'main', $chain[0];
    }

    my $collection = $self->{'collection'};

    my $last_column = pop @chain;
    my $last_alias = 'main';

    my %aliases;

    foreach my $column ( @chain ) {
        unless ( blessed $column ) {
            if ( my $tmp = $aliases{ refaddr $column } ) {
                ($last_alias, $last_column) = @$tmp;
            } else {
                ($last_alias, $last_column) = $self->resolve_join( @$column );
            }
            next;
        }

        my $name = $column->name;

        my $classname = $column->refers_to;
        unless ( $classname ) {
            die "column '$name' of is not a reference";
        }

        if ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' ) ) {
            my $item = $classname->new( handle => $collection->_handle )->new_item;
            my $right_alias = $collection->new_alias( $item );
            $collection->join(
                type    => 'left',
                alias1  => $last_alias,
                column1 => 'id',
                alias2  => $right_alias,
                column2 => $column->by || 'id',
            );
            $last_alias = $right_alias;
        }
        elsif ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' ) ) {
            my $item = $classname->new( handle => $collection->_handle );
            my $right_alias = $collection->new_alias( $item );
            $collection->join(
                type    => 'left',
                alias1  => $last_alias,
                column1 => $name,
                alias2  => $right_alias,
                column2 => $column->by || 'id',
            );
            $last_alias = $right_alias;
        }
        else {
            die "Column '$name' refers to '$classname' which is not record or collection";
        }
    }
    return ($last_alias, $last_column);
}

sub split_condition {
    my $self = shift;
    my $string = shift;
    my $aliases = shift;

    if ( $string =~ /^($re_column)\s*($re_sql_op_bin)\s*($re_value)$/o ) {
        my ($lhs, $op, $rhs) = ($self->find_column($1, $aliases), $2, $3);
        if ( $rhs =~ /^$re_delim$/ ) {
            $rhs =~ s/^["']//g;
            $rhs =~ s/["']$//g;
        }
        return { lhs => $lhs, op => $op, rhs => $rhs };
    }
    elsif ( $string =~ /^($re_column)\s*($re_sql_op_un)$/o ) {
        my ($lhs, $op, $rhs) = ($self->find_column($1, $aliases), $2, $3);
        ($op, $rhs) = split /\s*(?=null)/i, $op;
        return { lhs => $lhs, op => $op, rhs => $rhs };
    }
    elsif ( $string =~ /^($re_column)\s*($re_sql_op_bin)\s*($re_column)$/o ) {
        return { lhs => $self->find_column($1, $aliases), op => $2, rhs => $self->find_column($3, $aliases) };
    }
    else {
        die "$string is not a tisql condition";
    }
}

sub find_column {
    my $self = shift;
    my $string = shift;
    my $aliases = shift;

    my @res;

    my ($start_from, @names) = split /\./, $string;
    my $item;
    unless ( $start_from ) {
        $item = $self->{'collection'}->new_item;
    } else {
        my $alias = $aliases->{ $start_from } || die "$start_from alias is not defined";
        $item = $alias->[-1]->refers_to->new( handle => $self->{'collection'}->_handle );
        push @res, $alias;
    }
    while ( my $name = shift @names ) {
        my $column = $item->column( $name );
        die "$item has no column '$name'" unless $column;

        push @res, $column;
        return \@res unless @names;

        my $classname = $column->refers_to;
        unless ( $classname ) {
            die "column '$name' of $item is not a reference";
        }

        if ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' ) ) {
            $item = $classname->new( handle => $self->{'collection'}->_handle )->new_item;
        }
        elsif ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' ) ) {
            $item = $classname->new( handle => $self->{'collection'}->_handle )
        }
        else {
            die "Column '$name' refers to '$classname' which is not record or collection";
        }
    }

    return \@res;
}

sub filter_conditions_tree {
    my ($self, $tree, $cb, $inner) = @_;

    my $skip_next = 0;

    my @res;
    foreach my $entry ( @$tree ) {
        next if $skip_next-- > 0;

        if ( ref $entry eq 'ARRAY' ) {
            my $tmp = $self->filter_conditions( $entry, $cb, 1 );
            if ( !$tmp || (ref $tmp eq 'ARRAY' && !@$tmp) ) {
                pop @res;
                $skip_next = 1 unless @res;
            } else {
                push @res, $tmp;
            }
        } elsif ( ref $entry eq 'HASH' ) {
            if ( $cb->( $entry ) ) {
                push @res, $entry;
            } else {
                pop @res;
                $skip_next = 1 unless @res;
            }
        } else {
            push @res, $entry;
        }
    }
    return $res[0] if @res == 1 && ($inner || ref $res[0] eq 'ARRAY');
    return \@res;
}


1;
