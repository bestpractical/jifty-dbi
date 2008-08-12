package Jifty::DBI::Tisql;

use strict;
use warnings;

use base qw(Parse::BooleanLogic);
use Scalar::Util qw(refaddr blessed);

use Data::Dumper;

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

sub add_reference {
    my $self = shift;
    my %args = (
        model     => undef,
        name      => undef,
        refers_to => undef,
        tisql     => '',
        @_
    );
    $args{'model'} ||= ref($self->{'collection'}->new_item);
    my $column = Jifty::DBI::Column->new({
        name      => $args{'name'},
        refers_to => $args{'refers_to'},
        by        => $args{'by'},
        tisql     => $args{'tisql'},
        virtual   => 1,
    });
    $self->{'additional_columns'}{ $args{'model'} }{ $args{'name'} } = $column;
    return $self;
}

sub query {
    my $self = shift;
    my $string = shift;

    my $tree = {
        aliases => {},
        conditions => undef,
    };

    # parse "FROM..." prefix into $tree->{'aliases'}
    if ( $string =~ s/^\s*FROM\s+($re_alias(?:\s*,\s*$re_alias)*)\s+WHERE\s+//oi ) {
        $tree->{'aliases'}->{ $_->[1] } = $self->find_column( $_->[0], $tree->{'aliases'} )
            foreach map [split /\s+AS\s+/i, $_], split /\s*,\s*/, $1;
        while ( my ($name, $meta) = each %{ $tree->{aliases} } ) {
            $meta->{'name'} = $name;
        }
    }

    $tree->{'conditions'} = $self->as_array(
        $string,
        operand_cb => sub { return $self->parse_condition( $_[0], $tree->{'aliases'} ) },
    );
    $self->{'tisql'}{'conditions'} = $tree->{'conditions'};
    $self->apply_query_tree( $tree->{'conditions'} );
    return $self;
}

sub apply_query_tree {
    my $self = shift;
    my $tree = shift;
    my $current = shift || $tree->{'conditions'};
    my $ea = shift || 'AND';

    my $collection = $self->{'collection'};

    $collection->open_paren('tisql');
    foreach my $element ( @$current ) {
        unless ( ref $element ) {
            $ea = $element;
            next;
        }
        elsif ( ref $element eq 'ARRAY' ) {
            $self->apply_query_tree( $tree, $element, $ea );
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
            $limit{'alias'} = $self->resolve_join( $element->{'lhs'} );
            $limit{'column'} = $element->{'lhs'}{'chain'}[-1]->name;
        } else {
            die "left hand side must be always column specififcation";
        }
        if ( ref $element->{'rhs'} ) {
            $limit{'quote_value'} = 0;
            $limit{'value'} =
                $self->resolve_join( $element->{'rhs'} )
                .'.'. $element->{'rhs'}{'chain'}[-1]->name;
        } else {
            $limit{'value'} = $element->{'rhs'};
        }

        $collection->limit( %limit );
    }
    $collection->close_paren('tisql');
}

sub resolve_join {
    my $self = shift;
    my $meta = shift;

    return $meta->{'sql_alias'} if $meta->{'sql_alias'};

    my $collection = $self->{'collection'};

    my $last_alias = 'main';
    if ( my $prev = $meta->{'previous'} ) {
        $last_alias = $self->resolve_join( $prev );
    }

    my @chain = @{ $meta->{'chain'} };
    while( my $column = shift @chain ) {
        my $name = $column->name;

        my $classname = $column->refers_to;
        unless ( $classname ) {
            die "column '$name' is not a reference when there are still items in the chain"
                if @chain;
            return $meta->{'sql_alias'} = $last_alias;
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
    $meta->{'sql_alias'} = $last_alias;
    return $last_alias;
}

sub parse_condition {
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
    my $collection = shift || $self->{'collection'};

    my %res = (
        string   => $string,
        previous => undef,
        chain    => [],
    );

    my ($start_from, @names) = split /\./, $string;
    my $item;
    unless ( $start_from ) {
        $item = $collection->new_item;
    } else {
        my $alias = $aliases->{ $start_from }
            || die "$start_from alias is not declared in from clause";
        $res{'previous'} = $alias;
        my $classname = $alias->{'chain'}[-1]->refers_to; # ->new( handle => $collection->_handle );
        if ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' ) ) {
            $item = $classname->new( handle => $collection->_handle )->new_item;
        }
        elsif ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' ) ) {
            $item = $classname->new( handle => $collection->_handle )
        }
        else {
            die "Column refers to '$classname' which is not record or collection";
        }
    }
    while ( my $name = shift @names ) {
        my $column = $item->column( $name );
        die ref($item) ." has no column '$name'" unless $column;

        push @{ $res{'chain'} }, $column;
        return \%res unless @names;

        my $classname = $column->refers_to;
        unless ( $classname ) {
            die "column '$name' of ". ref($item) ." is not a reference to record or collection";
        }

        if ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' ) ) {
            $item = $classname->new( handle => $collection->_handle )->new_item;
        }
        elsif ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' ) ) {
            $item = $classname->new( handle => $collection->_handle )
        }
        else {
            die "Column '$name' refers to '$classname' which is not record or collection";
        }
    }

    return \%res;
}

sub filter_conditions_tree {
    my ($self, $tree, $cb, $inner) = @_;

    my $skip_next = 0;

    my @res;
    foreach my $entry ( @$tree ) {
        next if $skip_next-- > 0;

        if ( ref $entry eq 'ARRAY' ) {
            my $tmp = $self->filter_conditions_tree( $entry, $cb, 1 );
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

sub apply_callback_to_tree {
    my ($self, $tree, $cb) = @_;

    foreach my $entry ( @$tree ) {
        if ( ref $entry eq 'ARRAY' ) {
            $self->apply_callback_to_tree( $entry, $cb );
        } elsif ( ref $entry eq 'HASH' ) {
            $cb->( $entry );
        }
    }
}

sub parse_column_reference {
    my $self = shift;
    my %args = @_;

    my $record = $args{'record'};
    my $column = $args{'column'};
    my $string = $column->tisql;

    my $record_alias = {
        string    => 'record',
        previous  => undef,
        chain     => [$column],
        sql_alias => $self->{'collection'}->new_alias( $record ),
    };

    my $tree = {
        aliases => {
            record => $record_alias
        },
        conditions => undef,
    };

    $tree->{'conditions'} = $self->as_array(
        $string,
        operand_cb => sub { return $self->parse_condition( $_[0], $tree->{'aliases'} ) },
    );
    $tree->{'conditions'} = [
        {
            lhs => {
                string => 'record.id',
                previous => $record_alias,
                chain => [ $record->column('id') ]
            },
            op => '=',
            rhs => $record->id
        },
        'AND',
        $tree->{'conditions'},
    ];
    $self->{'tisql'}{'conditions'} = $tree->{'conditions'};
    $self->apply_query_tree( $tree );

    return $tree;
}

{
my %cache;
my $i = 0;
my $aliases;
my $merge_joins_cb = sub {
    my $meta = shift;
    my @parts = split /\./, $meta->{'string'};
    while ( @parts > 2 ) {
        my $new_str = join '.', splice @parts, 0, 2;
        my $m = $cache{ $new_str };
        unless ( $m ) {
            my $name = 'a'. ++$i;
            $name = "a". ++$i while exists $aliases->{ $name };
            $m = {
                name     => $name,
                string   => $new_str,
                chain    => [ $meta->{'chain'}[0] ],
                previous => $meta->{'previous'},
            };
            $cache{ $new_str } = $aliases->{ $name } = $m;
        }
        shift @{ $meta->{'chain'} };
        unshift @parts, $m->{'name'};
        $meta->{'previous'} = $m;
        $meta->{'string'} = join '.', @parts;
    }
};

sub merge_joins {
    my $self = shift;
    my $tree = shift;
    %cache = ();
    $aliases = $tree->{'aliases'};

    $merge_joins_cb->( $_ ) foreach values %$aliases;
    $self->apply_callback_to_tree(
        $tree->{'conditions'},
        sub {
            my $condition = shift;
            $merge_joins_cb->( $_ ) foreach
                grep ref $_, map $condition->{$_}, qw(lhs rhs);
        }
    );
}
}

1;
