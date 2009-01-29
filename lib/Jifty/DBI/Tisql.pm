package Jifty::DBI::Tisql;

use strict;
use warnings;

use Scalar::Util qw(refaddr blessed weaken);

use Data::Dumper;
use Carp ();


use Parse::BooleanLogic 0.07;
my $parser = new Parse::BooleanLogic;

use Regexp::Common qw(delimited);
my $re_delim      = qr{$RE{delimited}{-delim=>qq{\'\"}}};
my $re_field      = qr{[a-zA-Z][a-zA-Z0-9_]*};
my $re_alias_name = $re_field;
my $re_ph         = qr{%[1-9][0-9]*};
my $re_binding    = qr{\?};

my $re_value      = qr{$re_delim|[0-9.]+};
my $re_value_ph   = qr{$re_value|$re_ph};
my $re_value_ph_b = qr{$re_value_ph|$re_binding};
my $re_cs_values  = qr{$re_value(?:\s*,\s*$re_value)*};
my $re_ph_access  = qr{{\s*(?:$re_cs_values|$re_ph|$re_binding)?\s*}};
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

sub new {
    my $proto = shift;
    return bless { @_ }, ref($proto)||$proto;
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
    my $self   = shift;
    my $string = shift;
    my @binds  = @_;

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
    my $operand_cb = sub {
        return $self->parse_condition( 
            'query', $_[0], sub { $self->find_column( $_[0], $tree->{'aliases'} ) }
        );
    };
    $self->{'bindings'} = \@binds;
    $tree->{'conditions'} = $parser->as_array(
        $string, operand_cb => $operand_cb,
    );
    $self->{'bindings'} = undef;
    $self->{'tisql'}{'conditions'} = $tree->{'conditions'};
    $self->apply_query_tree( $tree->{'conditions'} );
    return $self;
}

sub apply_query_tree {
    my ($self, $tree, $join, $ea) = @_;
    $ea ||= 'AND';

    my $collection = $self->{'collection'};

    $collection->open_paren('tisql', $join);
    foreach my $element ( @$tree ) {
        unless ( ref $element ) {
            $ea = $element;
            next;
        }
        elsif ( ref $element eq 'ARRAY' ) {
            $self->apply_query_tree( $element, $join, $ea );
            next;
        }
        elsif ( ref $element eq 'HASH' ) {
            $self->apply_query_condition( $collection, $ea, $element, $join );
        } else {
            die "wrong query tree";
        }
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

    my $bundling = $long && !$join && $self->{'joins_bundling'};
    my $bundled = 0;
    if ( $bundling ) {
        my $bundles = $self->{'cache'}{'condition_bundles'}{ $condition->{'lhs'}{'string'} }{ $prefix } ||= [];
        foreach my $bundle ( @$bundles ) {
            my %tmp;
            $tmp{$_}++ foreach map refaddr($_), @$bundle;
            my $cur_refaddr = refaddr( $condition );
            if ( $prefix eq 'has' ) {
                next unless $parser->fsolve(
                    $self->{'tisql'}{'conditions'},
                    sub {
                        my $ra = refaddr($_[0]);
                        return 1 if $ra == $cur_refaddr;
                        return 0 if $tmp{ $ra };
                        return undef;
                    },
                );
            } else {
                next if $parser->fsolve(
                    $self->{'tisql'}{'conditions'},
                    sub {
                        my $ra = refaddr($_[0]);
                        return 1 if $ra == $cur_refaddr;
                        return 0 if $tmp{ $ra };
                        return undef;
                    },
                );
            }
            $condition->{'lhs'}{'previous'}{'sql_alias'} = $bundle->[-1]{'lhs'}{'previous'}{'sql_alias'};
            push @$bundle, $condition;
            $bundled = 1;
            last;
        }
        push @$bundles, [ $condition ] unless $bundled;
    }

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
                $parser->dq( $_ ) foreach @{ $condition->{'rhs'} };
            } else {
                $parser->dq( $condition->{'rhs'} );
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
                $parser->dq( $_ ) foreach @{ $condition->{'rhs'} };
            } else {
                $parser->dq( $condition->{'rhs'} );
            }
            $limit{'value'} = $condition->{'rhs'};
        }

        $collection->limit(
            %limit,
            entry_aggregator => $bundled? 'OR': 'AND',
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

    my $column = $meta->{'column'};

    my $refers = $meta->{'refers_to'};
    $refers = $refers->new_item
        if UNIVERSAL::isa( $refers, 'Jifty::DBI::Collection' );

    unless ( UNIVERSAL::isa( $refers, 'Jifty::DBI::Record' ) ) {
        die "Column '". $column->name ."' refers to '"
            . (ref($refers) || $refers)
            ."' that is not record or collection";
    }

    my $sql_alias = $meta->{'sql_alias'}
        = $collection->new_alias( $refers, 'LEFT' );

    if ( $column->tisql ) {
        local $self->{'right_part_of_join'} = {
            %$meta
        };
        $self->resolve_tisql_join( $sql_alias, $meta );
    } else {
        my %limit = (
            subclause   => 'tisql-join',
            alias       => $prev_alias,
            column      => $column->virtual? 'id' : $column->name,
            operator    => '=',
            quote_value => 0,
            value       => $sql_alias .'.'. ($column->by || 'id')
        );

        if ( $self->{'inside_left_join'} ) {
            $limit{'leftjoin'} = $sql_alias;
        } else {
            $limit{'leftjoin'} = $sql_alias;
        }
        $collection->limit( %limit );
    }
    return $sql_alias;
}

sub resolve_tisql_join {
    my $self = shift;
    my $alias = shift;
    my $meta = shift;

    my $tree = $parser->as_array(
        $meta->{'column'}->tisql,
        operand_cb => sub { return $self->parse_condition(
            'join', $_[0], sub { return $self->find_column(
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


    # fill in placeholders
    $tree = $parser->filter( $tree, sub {
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
            }
        }
        return 1;
    } );

#    Test::More::diag( Dumper $tree );

    $self->apply_join_tree( $tree, undef, $alias );
}

sub apply_join_tree {
    my ($self, $tree, $ea, $join, @rest) = @_;
    $ea ||= 'AND';

    my $collection = $self->{'collection'};
    $collection->open_paren('tisql', $join);
    foreach my $element ( @$tree ) {
        unless ( ref $element ) {
            $ea = $element;
            next;
        }
        elsif ( ref $element eq 'ARRAY' ) {
            $self->apply_join_tree( $element, $ea, $join, @rest );
            next;
        }
        elsif ( ref $element eq 'HASH' ) {
            Test::More::diag( Dumper($element) );
            if ( $element->{'lhs'}{'string'} =~ /^([^.]+)\.[^.]+\./ ) {
                # it's subjoin in join: a column described using tisql and has more
                # than just target.x = .source, but something like: target.x.y = ...

                # here we have
                my $alias = $1;

                $collection->open_paren('tisql', $join);

                die "here we are";

                $collection->close_paren('tisql', $join);
            }
            $self->apply_join_condition( $collection, $ea, $element, $join, @rest );
        } else {
            die "wrong query tree";
        }
    }
    $collection->close_paren('tisql', $join);
}

sub apply_join_condition {
    my ($self, $collection, $ea, $condition, $join) = @_;

    die "left hand side must be always column specififcation"
        unless ref $condition->{'lhs'} eq 'HASH';


    my $op = $condition->{'op'};
    if ( $condition->{'prefix'} && $condition->{'prefix'} eq 'has no' ) {
        die "'has' and 'has no' prefixes are only allowed in query, not in joins";
    }

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
            $parser->dq( $_ ) foreach @{ $condition->{'rhs'} };
        } else {
            $parser->dq( $condition->{'rhs'} );
        }
        $limit{'value'} = $condition->{'rhs'};
    }

    $collection->limit( %limit );
}

sub describe_join {
    my $self  = shift;
    my $model = shift;
    my $via   = shift;

    $model = UNIVERSAL::isa( $model, 'Jifty::DBI::Collection' )
        ? $model->new_item
        : $model;

    my $column = $model->column( $via )
        or die "no column";

    my $refers_to = $column->refers_to->new;
    $refers_to = $refers_to->new_item
        if $refers_to->isa('Jifty::DBI::Collection');

    my $tree;
    if ( my $tisql = $column->tisql ) {
        $tree = $parser->as_array( $tisql, operand_cb => sub {
            return $self->parse_condition(
                'join', $_[0], sub { $self->parse_column( $_[0] ) }
            )
        } );
    } else {
        $tree = [ {
            type    => 'join',
            op_type => 'col_op_col',
            lhs => {
                alias  => '',
                chain  => [{ name => $via }],
            },
            op => '=',
            rhs => {
                alias  => $via,
                chain  => [{ name => $column->by || 'id' }],
            },
        } ];
        foreach ( map $tree->[0]{$_}, qw(lhs rhs) ) {
            $_->{'chain'}[0]{'string'} = $_->{'alias'} .'.'. $_->{'chain'}[0]{'name'};
            $_->{'string'} = $_->{'chain'}[-1]{'string'};
        }
        $tree->[0]{'string'} =
            join ' ',
            $tree->[0]{'lhs'}{'string'},
            $tree->[0]{'op'},
            $tree->[0]{'rhs'}{'string'};
    }
    my $res = {
        left => {
            model => $model,
            column => $column,
        },
        right => {
            model => $refers_to,
        },
        tree => $tree,
    };
    return $res;
}

sub linearize_join {
    my $self = shift;
    my $join = shift;
    my $inverse = shift;
    my $attach_to = shift;
    my $place_of_attachment = shift;

    my @res (
        $attach_to || { model => $join->{'left'}{'model'} },
        { model => $join->{'right'}{'model'} },
    );
    my ($left, $right) = @res;

    my $transfer_short = sub {
        my %new = ();
        $new{'table'} = $_[0]->{'alias'}? $right : $left;
        weaken($new{'table'});
        $new{'column'} = $_[0]->{'chain'}[0]{'name'};
        return \%new;
    };


    my ($tree, $node, @pnodes);
    my %callback;
    $callback{'open_paren'} = sub {
        push @pnodes, $node;
        push @{ $pnodes[-1] }, $node = []
    };
    $callback{'close_paren'} = sub { $node = pop @pnodes };
    $callback{'operator'}    = sub { push @$node, $_[0] };
    $callback{'operand'}     = sub {
        my $cond = $_[0];
        my %new_cond = %$cond;

        if ( $cond->{'op_type'} eq 'col_op_col' ) {
            if ( !$cond->{'lhs'}{'is_long'} && !$cond->{'rhs'}{'is_long'} ) {
                foreach my $side (qw(lhs rhs)) {
                    $new_cond{$side} = $transfer_short->( $cond->{$side} );
                }
            }
        } else {
            unless ( $cond->{'lhs'}{'is_long'} ) {
                $new_cond{'lhs'} = $transfer_short->( $cond->{'lhs'} );
            } else {
                my @chain = @{ $cond->{'lhs'}{'chain'} };
                my $last_column = pop @chain;

                my $conditions = [];

                my $model = ($cond->{'lhs'}{'alias'}? $right : $left)->{'model'};
                foreach my $ref ( @chain ) {
                    my $description = $self->describe_join( $model => $ref->{'name'} );
                    my $linear = $self->linearize_join(
                        $description,
                        $cond->{'lhs'}{'alias'}
                            ? ('inverse', $right, $conditions)
                            : (undef, $left)
                    );

                    $model = $model->column( $ref->{'name'} )->refers_to->new;
                }
            }
        }
        push @$node, \%new_cond;
    };

    $tree = $node = [];
    $parser->walk( $join->{'tree'}, \%callback );

    @res = reverse @res if $inverse;

    if ( $place_of_attachment ) {
        push @{ $place_of_attachment }, $tree;
    } else {
        $res[-1]{'conditions'} = $tree;
    }
    return \@res;
}

sub _linearize_join {
    my ($self, $tree, $ea, $join, @rest) = @_;
    $ea ||= 'AND';

    my $collection = $self->{'collection'};
    $collection->open_paren('tisql', $join);
    foreach my $element ( @$tree ) {
        unless ( ref $element ) {
            $ea = $element;
            next;
        }
        elsif ( ref $element eq 'ARRAY' ) {
            $self->apply_join_tree( $element, $ea, $join, @rest );
            next;
        }
        elsif ( ref $element eq 'HASH' ) {
            Test::More::diag( Dumper($element) );
            if ( $element->{'lhs'}{'string'} =~ /^([^.]+)\.[^.]+\./ ) {
                # it's subjoin in join: a column described using tisql and has more
                # than just target.x = .source, but something like: target.x.y = ...

                # here we have
                my $alias = $1;

                $collection->open_paren('tisql', $join);

                die "here we are";

                $collection->close_paren('tisql', $join);
            }
            $self->apply_join_condition( $collection, $ea, $element, $join, @rest );
        } else {
            die "wrong query tree";
        }
    }
    $collection->close_paren('tisql', $join);
}

sub parse_condition {
    my ($self, $type, $string, $cb) = @_;

    my %res = (
        string   => $string,
        type     => $type,
        op_type  => undef,    # 'col_op', 'col_op_val' or 'col_op_col'
        modifier => '',       # '', 'has' or 'has no'
        lhs      => undef,
        op       => undef,
        rhs      => undef,
    );

    if ( $type eq 'query' ) {
        # TODO: query can not have placeholders %##
        if ( $string =~ /^(has(\s+no)?\s+)?($re_column)\s*($re_sql_op_bin)\s*($re_value_ph_b)$/io ) {
            $res{'modifier'} = $2? 'has no': $1? 'has': '';
            @res{qw(op_type lhs op rhs)} = ('col_op_val', $cb->($3), $4, $5);
        }
        elsif ( $string =~ /^($re_column)\s*($re_sql_op_un)$/o ) {
            my ($lhs, $op) = ($cb->($1), $2);
            @res{qw(op_type lhs op rhs)} = ('col_op', $lhs, split /\s*(?=null)/i, $op );
        }
        elsif ( $string =~ /^(has(\s+no)?\s+)?($re_column)\s*($re_sql_op_bin)\s*($re_column)$/o ) {
            $res{'modifier'} = $2? 'has no': $1? 'has': '';
            @res{qw(op_type lhs op rhs)} = ('col_op_col', $cb->($3), $4, $cb->($5));
        }
        elsif ( $string =~ /^has(\s+no)?\s+($re_column)$/o ) {
            @res{qw(op_type lhs op rhs)} = ('col_op', $cb->( $2 .'.id' ), $1? 'IS': 'IS NOT', 'NULL');
        }
        else {
            die "$string is not a tisql $type condition";
        }
    }
    elsif ( $type eq 'join' ) {
        # TODO: join can not have bindings (?)
        if ( $string =~ /^($re_column)\s*($re_sql_op_bin)\s*($re_value_ph_b)$/io ) {
            @res{qw(op_type lhs op rhs)} = ('col_op_val', $cb->($1), $2, $3);
        }
        elsif ( $string =~ /^($re_column)\s*($re_sql_op_un)$/o ) {
            my ($lhs, $op) = ($cb->($1), $2);
            @res{qw(op_type lhs op rhs)} = ('col_op', $lhs, split /\s*(?=null)/i, $op );
        }
        elsif ( $string =~ /^($re_column)\s*($re_sql_op_bin)\s*($re_column)$/o ) {
            @res{qw(op_type lhs op rhs)} = ('col_op_col', $cb->($1), $2, $cb->($3));
        }
        else {
            die "$string is not a tisql $type condition";
        }
    }
    else {
        die "$type is not valid type of a condition";
    }
    return \%res;
}

sub check_query_condition {
    my ($self, $cond) = @_;

    die "Last column in '". $cond->{'lhs'}{'string'} ."' is virtual" 
        if $cond->{'lhs'}{'column'}->virtual;

    if ( $cond->{'op_type'} eq 'col_op_col' ) {
        die "Last column in '". $cond->{'rhs'}{'string'} ."' is virtual" 
            if $cond->{'rhs'}{'column'}->virtual;
    }

    return $cond;
}


# returns something like:
# {
#   'string'  => 'nodes.attr{"category"}.value',
#   'alias'   => 'nodes',                            # alias or ''
#   'is_long' => 1,                                  # 1 or 0
#   'chain'   => [
#        {
#          'name' => 'attr',
#          'string' => 'nodes.attr{"category"}',
#          'placeholders' => ['"category"'],
#        },
#        {
#          'name' => 'value',
#          'string' => 'nodes.attr{"category"}.value'
#        }
#   ],
# }
# no look ups, everything returned as is,
# even placeholders' strings are not de-escaped

sub parse_column {
    my $self = shift;
    my $string = shift;

    my (%res, @columns);
    $res{'string'} = $string;
    ($res{'alias'}, @columns) = split /\.($re_field$re_ph_access*)/o, $string;
    @columns = grep defined && length, @columns;
    $res{'is_long'} = @columns > 1? 1 : 0;

    my $prev = $res{'alias'};
    foreach my $col (@columns) {
        my $string = $col;
        $col =~ s/^($re_field)//;
        my $field = $1;
        my @phs = split /{\s*($re_cs_values|$re_ph)?\s*}/, $col;
        @phs = grep !defined || length, @phs;
        $col = {
            name => $field,
            string => $prev .".$string",
        };
        $col->{'placeholders'} = \@phs if @phs;
        foreach my $ph ( grep defined, @phs ) {
            if ( $ph =~ /^%([0-9]+)$/ ) {
                $ph = $1;
            }
            elsif ( $ph eq '?' ) {
                $ph = '?';
            }
            else {
                my @values;
                while ( $ph =~ s/^($re_value)\s*,?\s*// ) {
                    push @values, $1;
                }
                $ph = \@values;
            }
        }
        $prev = $col->{'string'};
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
    my $conditions = $parser->as_array(
        $column->tisql,
        operand_cb => sub {
            return $self->parse_condition( 'join', $_[0], $column_cb )
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


1;
