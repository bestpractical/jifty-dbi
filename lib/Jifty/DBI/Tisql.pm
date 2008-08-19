package Jifty::DBI::Tisql;

use strict;
use warnings;

use base qw(Parse::BooleanLogic);
use Scalar::Util qw(refaddr blessed);

use Data::Dumper;

use Regexp::Common qw(delimited);
my $re_delim      = qr{$RE{delimited}{-delim=>qq{\'\"}}};
my $re_field      = qr{[a-zA-Z][a-zA-Z0-9_]*};
my $re_alias_name = $re_field;
my $re_ph         = qr{%[1-9][0-9]*};

my $re_value      = qr{$re_delim|[0-9.]+};
my $re_value_ph   = qr{$re_value|$re_ph};
my $re_cs_values  = qr{$re_value(?:\s*,\s*$re_value)*};
my $re_ph_access  = qr{{\s*(?:$re_cs_values|$re_ph)?\s*}};
my $re_column     = qr{$re_alias_name?(?:\.$re_field$re_ph_access*)+};
my $re_alias      = qr{$re_column\s+AS\s+$re_alias_name}i;

my $re_sql_op_bin = qr{!?=|<>|>=?|<=?|(?:NOT )?LIKE}i;
my $re_sql_op_un  = qr{IS (?:NOT )?NULL}i;
my $re_positive_op = qr/^(?:=|IS|LIKE)$/i;
my $re_negative_op = qr/^(?:!=|<>|NOT LIKE)$/i;
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

sub dq {
    my $s = $_[0];
    return $s unless $s =~ /^$re_delim$/o;
    substr( $s, 0, 1 ) = '';
    substr( $s, -1   ) = '';
    $s =~ s/\\(?=["'])//g;
    return $s;
}

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
        record_class => $args{'model'},
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
        operand_cb => sub { return $self->parse_condition( 
            $_[0], sub { $self->find_column( $_[0], $tree->{'aliases'} ) }
        ) },
    );
    $self->{'tisql'}{'conditions'} = $tree->{'conditions'};
    $self->apply_query_tree( $tree->{'conditions'} );
    return $self;
}

sub apply_query_tree {
    my ($self, $current, $join, $ea) = @_;
    $ea ||= 'AND';

    my $collection = $self->{'collection'};

    $collection->open_paren('tisql', $join);
    foreach my $element ( @$current ) {
        unless ( ref $element ) {
            $ea = $element;
            next;
        }
        elsif ( ref $element eq 'ARRAY' ) {
            $self->apply_query_tree( $element, $join, $ea );
            next;
        }
        elsif ( ref $element ne 'HASH' ) {
            die "wrong query tree";
        }

        $self->apply_query_condition( $collection, $ea, $element, $join );
    }
    $collection->close_paren('tisql', $join);
}

sub apply_query_condition {
    my ($self, $collection, $ea, $condition, $join) = @_;

    die "left hand side must be always column specififcation"
        unless ref $condition->{'lhs'} eq 'HASH';

    my $prefix = $condition->{'prefix'};
    my $op     = $condition->{'op'};
    my $long   = do {
        my @tmp = split /\./, $condition->{'lhs'}{'string'};
        @tmp > 2 ? 1 : 0
    };
    if ( $long && !$prefix && $op =~ $re_negative_op ) {
        $prefix = 'has no';
        $op = $invert_op{ lc $op };
    }
    elsif ( $prefix && !$long ) {
        die "'has no' and 'has' prefixes are only applicable on columns of related records";
    }
    $prefix ||= 'has';

    if ( $prefix eq 'has' ) {
        my %limit = (
            subclause        => 'tisql',
            leftjoin         => $join,
            entry_aggregator => $ea,
            alias            => $self->resolve_join( $condition->{'lhs'} ),
            column           => $condition->{'lhs'}{'column'}->name,
            operator         => $op,
        );
        if ( ref $condition->{'rhs'} eq 'HASH' ) {
            $limit{'quote_value'} = 0;
            $limit{'value'} =
                $self->resolve_join( $condition->{'rhs'} )
                .'.'. $condition->{'rhs'}{'column'}->name;
        } else {
            if ( ref $condition->{'rhs'} eq 'ARRAY' ) {
                $_ = dq( $_ ) foreach @{ $condition->{'rhs'} };
            } else {
                $condition->{'rhs'} = dq( $condition->{'rhs'} );
            }
            $limit{'value'} = $condition->{'rhs'};
        }

        $collection->limit( %limit );
    }
    else {
        die "not yet implemented: it's a join" if $join;

        my %limit = (
            subclause        => 'tisql',
            alias            => $self->resolve_join( $condition->{'lhs'} ),
            column           => $condition->{'lhs'}{'column'}->name,
            operator         => $op,
        );
        if ( ref $condition->{'rhs'} eq 'HASH' ) {
            $limit{'quote_value'} = 0;
            $limit{'value'} =
                $self->resolve_join( $condition->{'rhs'} )
                .'.'. $condition->{'rhs'}{'column'}->name;
        } else {
            if ( ref $condition->{'rhs'} eq 'ARRAY' ) {
                $_ = dq( $_ ) foreach @{ $condition->{'rhs'} };
            } else {
                $condition->{'rhs'} = dq( $condition->{'rhs'} );
            }
            $limit{'value'} = $condition->{'rhs'};
        }

        $collection->limit(
            %limit,
            entry_aggregator => 'AND',
            leftjoin         => $limit{'alias'},
        );

        $collection->limit(
            subclause        => 'tisql',
            entry_aggregator => $ea,
            alias            => $limit{'alias'},
            column           => 'id',
            operator         => 'IS',
            value            => 'NULL',
            quote_value      => 0,
        );
    }
}

sub resolve_join {
    my $self = shift;
    my $meta = shift;
    my $resolve_last = shift;

    return $meta->{'sql_alias'}
        if $meta->{'sql_alias'} && $resolve_last;

    my $collection = $self->{'collection'};

    my ($prev_alias) = ('main');
    if ( my $prev = $meta->{'previous'} ) {
        $prev_alias = $self->resolve_join( $prev, 'resolve_last' );
    }
    return $prev_alias unless $resolve_last;

    my $res;
    my $column = $meta->{'column'};
    my $refers = $meta->{'refers_to'};
    if ( UNIVERSAL::isa( $refers, 'Jifty::DBI::Collection' ) ) {
        my $item = $refers->new_item;
        if ( my $tisql = $column->tisql ) {
            $res = $self->resolve_tisql_join( $meta );
        } else {
            $res = $collection->new_alias( $item );
            $collection->join(
                subclause => 'tisql',
                type    => 'left',
                alias1  => $prev_alias,
                column1 => 'id',
                alias2  => $res,
                column2 => $column->by || 'id',
            );
        }
    }
    elsif ( UNIVERSAL::isa( $refers, 'Jifty::DBI::Record' ) ) {
        $res = $collection->new_alias( $refers );
        $collection->join(
            subclause => 'tisql',
            type    => 'left',
            alias1  => $prev_alias,
            column1 => $column->name,
            alias2  => $res,
            column2 => $column->by || 'id',
        );
    }
    else { 
        die "Column '". $column->name ."' refers to '"
            . (ref($refers) || $refers)
            ."' that is not record or collection";
    }
    return $res;
}

sub resolve_tisql_join {
    my $self = shift;
    my $meta = shift;

    my $alias = $self->{'collection'}->new_alias(
        $meta->{'refers_to'}->new_item
    );

    my $tree = $self->as_array(
        $meta->{'column'}->tisql,
        operand_cb => sub { return $self->parse_condition( 
            $_[0], sub { return $self->find_column(
                $_[0],
                {
                    '' => $meta->{'previous'},
                    $meta->{'column'}->name => {
                        %$meta,
                        sql_alias => $alias,
                    } 
                },
            ) }
        ) },
    );

    $tree = $self->filter_conditions_tree( $tree, sub {
        my $rhs = $_[0]->{'rhs'};
        if ( $rhs && !ref $rhs && $rhs =~ /^%([0-9]+)$/ ) {
            return 0 unless defined $meta->{'placeholders'}[ $1 - 1 ];
            $_[0]->{'rhs'} = $meta->{'placeholders'}[ $1 - 1 ];
            return 1;
        }
        foreach my $col ( grep ref $_ eq 'HASH', $rhs, $_[0]->{'lhs'} ) {
            my $tmp = $col;
            while ( $tmp ) {
                if ( my $phs = $tmp->{'placeholders'} ) {
                    for ( my $i = 0; $i < @$phs; $i++ ) {
                        my $ph = $phs->[$i];
                        next unless defined $ph;
                        next if ref $ph;
                        $phs->[$i] = $meta->{'placeholders'}[ $ph - 1 ];
                    }
                }
                $tmp = $tmp->{previous};
            };
        }
        return 1;
    } );

    $self->apply_query_tree( $tree, $alias );

    return $alias;
}

sub parse_condition {
    my $self = shift;
    my $string = shift;
    my $cb = shift;

    if ( $string =~ /^(has(\s+no)?\s+)?($re_column)\s*($re_sql_op_bin)\s*($re_value_ph)$/io ) {
        my ($lhs, $op, $rhs) = ($cb->($3), $4, $5);
        my $prefix;
        $prefix = 'has' if $1;
        $prefix .= ' no' if $2;
        die "Last column in '". $lhs->{'string'} ."' is virtual and can not be used in condition '$string'" 
            if $lhs->{'column'}->virtual;
        return { string => $string, prefix => $prefix, lhs => $lhs, op => $op, rhs => $rhs };
    }
    elsif ( $string =~ /^($re_column)\s*($re_sql_op_un)$/o ) {
        my ($lhs, $op, $rhs) = ($cb->($1), $2, $3);
        ($op, $rhs) = split /\s*(?=null)/i, $op;
        die "Last column in '". $lhs->{'string'} ."' is virtual and can not be used in condition '$string'" 
            if $lhs->{'column'}->virtual;
        return { string => $string, lhs => $lhs, op => $op, rhs => $rhs };
    }
    elsif ( $string =~ /^(has(\s+no)?\s+)?($re_column)\s*($re_sql_op_bin)\s*($re_column)$/o ) {
        my ($lhs, $op, $rhs) = ($cb->($3), $4, $cb->($5));
        my $prefix;
        $prefix = 'has' if $1;
        $prefix .= ' no' if $2;
        die "Last column in '". $lhs->{'string'} ."' is virtual and can not be used in condition '$string'" 
            if $lhs->{'column'}->virtual;
        die "Last column in '". $rhs->{'string'} ."' is virtual and can not be used in condition '$string'" 
            if $rhs->{'column'}->virtual;
        return { string => $string, prefix => $prefix, lhs => $lhs, op => $op, rhs => $rhs };
    }
    elsif ( $string =~ /^has(\s+no)?\s+($re_column)$/o ) {
        return { string => $string, lhs => $cb->( $2 .'.id' ), op => $1? 'IS NOT': 'IS', rhs => 'NULL' };
    }
    else {
        die "$string is not a tisql condition";
    }
}

sub parse_column {
    my $self = shift;
    my $string = shift;

    my (%res, @columns);
    ($res{'alias'}, @columns) = split /\.($re_field$re_ph_access*)/o, $string;
    @columns = grep defined && length, @columns;
    my $prev;
    foreach my $col (@columns) {
        my $string = $col;
        $col =~ s/^($re_field)//;
        my $field = $1;
        my @phs = split /{\s*($re_cs_values|$re_ph)?\s*}/, $col;
        @phs = grep !defined || length, @phs;
        $col = {
            name => $field,
            string => ($prev? $prev->{'string'} : $res{'alias'}) .".$string",
        };
        $col->{'placeholders'} = \@phs if @phs;
        foreach my $ph ( grep defined, @phs ) {
            if ( $ph =~ /^%([0-9]+)$/ ) {
                $ph = $1;
            }
            else {
                my @values;
                while ( $ph =~ s/^($re_value)\s*,?\s*// ) {
                    push @values, $1;
                }
                $ph = \@values;
            }
        }
        $prev = $col;
    }
    $res{'chain'} = \@columns;
    return \%res;
}

sub find_column {
    my $self = shift;
    my $string = shift;
    my $aliases = shift;
    my $collection = shift || $self->{'collection'};

    my $meta = $self->parse_column($string);
    my $start_from = $meta->{'alias'};
    my ($item, $last);
    if ( !$start_from && !$aliases->{''} ) {
        $item = $collection->new_item;
    } else {
        my $alias = $aliases->{ $start_from }
            || die "alias '$start_from' is not declared";

        $last = $alias;
        $item = $alias->{'refers_to'};
        unless ( $item ) {
            die "last column in alias '$start_from' is not a reference";
        }
        $item = $item->new_item if $item->isa('Jifty::DBI::Collection');
    }

    my @chain = @{ $meta->{'chain'} };

    while ( my $joint = shift @chain ) {
        my $name = $joint->{'name'};
        my $column =
            $self->{'additional_columns'}{ref $item}{$name}
            || $item->column( $name );
        die ref($item) ." has no column '$name'" unless $column;

        my %res = (
            string       => $joint->{'string'},
            previous     => $last,
            column       => $column,
            placeholders => $joint->{'placeholders'},
        );

        my $classname = $column->refers_to;
        if ( !$classname && @chain ) {
            die "column '$name' of ". ref($item) ." is not a reference, but used so in '$string'";
        }
        return \%res unless $classname;

        if ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' ) ) {
            $res{'refers_to'} = $classname->new( handle => $collection->_handle );
            $item = $res{'refers_to'}->new_item;
        }
        elsif ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' ) ) {
            $res{'refers_to'} = $item = $classname->new( handle => $collection->_handle )
        }
        else {
            die "Column '$name' refers to '$classname' which is not record or collection";
        }
        $last = \%res;
    }

    return $last;
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

sub external_reference {
    my $self = shift;
    my %args = @_;

    my $record = $args{'record'};
    my $column = $args{'column'};
    my $name   = $column->name;

    my $aliases = { __record__ => {
        string    => '__record__',
        previous  => undef,
        column    => $column,
        refers_to => $column->refers_to->new( handle => $self->{'collection'}->_handle ),
        sql_alias => $self->{'collection'}->new_alias( $record ),
    } };

    my $column_cb = sub {
        my $str = shift;
        $str = "__record__". $str if 0 == rindex $str, '.', 0;
        substr($str, 0, length($name)) = '' if 0 == rindex $str, "$name.", 0;
        return $self->find_column($str, $aliases);
    };
    my $conditions = $self->as_array(
        $column->tisql,
        operand_cb => sub {
            return $self->parse_condition( $_[0], $column_cb )
        },
    );
    $conditions = [
        $conditions, 'AND',
        {
            lhs => {
                string   => '__record__.id',
                previous => $aliases->{'__record__'},
                column   => $record->column('id'),
            },
            op => '=',
            rhs => $record->id || 0,
        },
    ];
    $self->apply_query_tree( $conditions );

    return $self;
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
                column    => $meta->{'column'},
                previous => $meta->{'previous'},
            };
            $cache{ $new_str } = $aliases->{ $name } = $m;
        }
        # XXX: no more chain
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
