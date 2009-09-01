use strict;
use warnings;

package Jifty::DBI::Tisql;

use Scalar::Util qw(refaddr blessed weaken);

use Data::Dumper;
use Carp ();

use base 'Exporter';
our @EXPORT = ();
our @EXPORT_OK = qw(C Q);

use Parse::BooleanLogic 0.09;
my $parser = new Parse::BooleanLogic;

use Regexp::Common qw(delimited);
my $re_delim      = qr{$RE{delimited}{-delim=>qq{\'\"}}};
my $re_field      = qr{[a-zA-Z][a-zA-Z0-9_]*};
my $re_alias_name = $re_field;
my $re_ph_name    = qr{[a-z][a-z_]*};
my $re_ph         = qr{%$re_ph_name};
my $re_binding    = qr{\?};

my $re_value      = qr{$re_delim|[0-9.]+};
my $re_value_ph   = qr{$re_value|$re_ph};
my $re_value_ph_b = qr{$re_value_ph|$re_binding};
my $re_cs_values  = qr{$re_value(?:\s*,\s*$re_value)*};
my $re_ph_access  = qr{{\s*$re_ph_name\s*=>\s*(?:$re_cs_values|$re_ph|$re_binding)\s*}};
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

sub get_reference {
    my $self = shift;
    my $model = shift;
    my $name = shift || die "illegal column name";

    my $res = $self->{'additional_columns'}{ ref($model) || $model }{ $name };
    $res ||= $model->column( $name );

    die "no column '$name' on model ". (ref($model) || $model)
        unless $res;

    return $res;
}

sub C { return Jifty::DBI::Tisql::Column->from_struct(@_) }
sub Q { return Jifty::DBI::Tisql::Condition->from_struct(@_) }

sub query {
    my $self = shift;
    return $self->query_struct(@_)
        if blessed $_[0]
        && ($_[0]->isa('Jifty::DBI::Tisql::Condition')
            || $_[0]->isa('Jifty::DBI::Tisql::Tree'));
    return $self->query_string(@_);
}

sub query_string {
    my $self   = shift;
    my $string = shift;
    my @binds  = @_;

    # parse "FROM..." prefix into aliases
    my %aliases;
    if ( $string =~ s/^\s*FROM\s+($re_alias(?:\s*,\s*$re_alias)*)\s+WHERE\s+//oi ) {
        $aliases{ $_->[1] } = $self->parse_column( $_->[0] )
            foreach map [split /\s+AS\s+/i, $_], split /\s*,\s*/, $1;
    }
    $self->{'aliases'} = \%aliases;

    my $tree = {};

    local $self->{'bindings'} = \@binds;
    $tree->{'conditions'} = $parser->as_array(
        $string, operand_cb => sub {
            return $self->parse_condition( 
                'query', $_[0], sub { $self->parse_column( $_[0] ) }
            );
        },
    );

    $self->{'tisql'}{'conditions'} = $tree->{'conditions'};
    $self->apply_query_tree( $tree->{'conditions'} );
    return $self;
}

sub query_struct {
    my $self = shift;

    my $tree = shift;
    $tree = $tree->clone;

    $self->{'tisql'}{'conditions'} = [ $tree->isa('HASH')? $tree : @$tree ]; #xxx: hack :)
    $self->apply_query_tree( $self->{'tisql'}{'conditions'} );
    return $self;
}

sub apply_query_tree {
    my ($self, $tree, $ea) = @_;
    $ea ||= 'AND';

    my $collection = $self->{'collection'};

    $collection->open_paren('tisql');
    foreach my $element ( @$tree ) {
        if ( ref $element eq 'ARRAY' ) {
            $self->apply_query_tree( $element, $ea );
        }
        elsif ( ref $element ) {
            $self->apply_query_condition( $collection, $ea, $element );
        }
        else {
            $ea = $element;
        }
    }
    $collection->close_paren('tisql');
    return $self;
}

sub apply_query_condition {
    my ($self, $collection, $ea, $condition) = @_;

    die "left hand side must be always column specififcation"
        unless ref $condition->{'lhs'} eq 'Jifty::DBI::Tisql::Column';

    my $modifier = $condition->{'modifier'};
    my $op       = $condition->{'op'};
    my $long     = @{ $condition->{'lhs'}{'chain'} } > 1;

    if ( $long && !$modifier && $op =~ $re_negative_op ) {
        $modifier = 'has no';
        $op = $invert_op{ lc $op };
    }
    elsif ( $modifier && !$long ) {
        die "'has no' and 'has' prefixes are only applicable on columns of related records";
    }
    $modifier ||= 'has';

    my $bundling = $long && $self->{'joins_bundling'};
    my $bundled = 0;
    if ( $bundling ) {
        my $bundles = $self->{'cache'}{'condition_bundles'}{ $condition->{'lhs'} }{ $modifier } ||= [];
        foreach my $bundle ( @$bundles ) {
            my %tmp;
            $tmp{$_}++ foreach map refaddr($_), @$bundle;
            my $cur_refaddr = refaddr( $condition );
            if ( $modifier eq 'has' ) {
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

    if ( $modifier eq 'has' ) {
        my %limit = (
            subclause        => 'tisql',
            entry_aggregator => $ea,
            alias            => $self->resolve_join( $condition->{'lhs'}, use_subjoin => (($op =~ /^is\b/i)? 1 : 0) ),
            column           => $condition->{'lhs'}{'chain'}[-1]{'name'},
            operator         => $op,
        );
        if ( ref $condition->{'rhs'} eq 'HASH' || ref $condition->{'rhs'} eq 'Jifty::DBI::Tisql::Column' ) {
            $limit{'quote_value'} = 0;
            $limit{'value'} =
                $self->resolve_join( $condition->{'rhs'} )
                .'.'. $condition->{'rhs'}{'chain'}[-1]{'name'};
        } elsif ( ref $condition->{'rhs'} eq 'ARRAY' ) {
            $parser->dq( $_ ) foreach @{ $condition->{'rhs'} };
            $limit{'value'} = $condition->{'rhs'};
        } elsif ( $condition->{'rhs'} eq '?' ) {
            die "Not enough binding values provided for the query"
                unless @{ $self->{'bindings'} };
            $limit{'value'} = shift @{ $self->{'bindings'} };
        } else {
            $parser->dq( $condition->{'rhs'} );
            $limit{'value'} = $condition->{'rhs'};
        }

        $collection->limit( %limit );
    }
    else {
        my %limit = (
            subclause        => 'tisql',
            alias            => $self->resolve_join( $condition->{'lhs'}, use_subjoin => 1 ),
            column           => $condition->{'lhs'}{'chain'}[-1]{'name'},
            operator         => $op,
        );
        if ( ref $condition->{'rhs'} eq 'HASH' || ref $condition->{'rhs'} eq 'Jifty::DBI::Tisql::Column' ) {
            $limit{'quote_value'} = 0;
            $limit{'value'} =
                $self->resolve_join( $condition->{'rhs'} )
                .'.'. $condition->{'rhs'}{'chain'}[-1]{'name'};
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
    my %args = (
        aliases => undef,
        resolve_last => 0,
        use_subjoin => 0,
        @_
    );
    my $aliases = $args{'aliases'} || $self->{'aliases'} || {};

    return $meta->{'chain'}[-1]{'sql_alias'}
        if $args{'resolve_last'} && $meta->{'chain'}[-1]{'sql_alias'};

    if ( my $prev = $meta->{'chain'}[-2] ) {
        return $prev->{'sql_alias'} if !$args{'resolve_last'} && $prev->{'sql_alias'};
    }

    my $collection = $self->{'collection'};

    my %last;
    if ( my $alias = $meta->{'alias'} ) {
        die "Couldn't find alias $alias"
            unless $aliases->{ $alias };
        my $target = $aliases->{ $alias }->qualify( tisql => $self, aliases => $aliases, new => 0 );
        my $item = $target->{'chain'}[-1]{'refers_to'}
            or die "Last column of alias '$alias' is not a reference";
        %last = (
            sql_alias => $self->resolve_join( $aliases->{ $alias }, aliases => $aliases, resolve_last => 1 ),
            item => $item,
        );
    } else {
        %last = (
            sql_alias => 'main',
            item => $collection,
        );
    }

    my @chain = @{ $meta->{'chain'} };
    pop @chain unless $args{'resolve_last'};

    my @aliases = ();
    while ( my $joint = shift @chain ) {
        my $linear = $self->linearize_join( $last{'item'}, $joint->{'name'}, $joint->{'placeholders'} );

        $linear->[0]{'sql_alias'} = $last{'sql_alias'};
        foreach ( @{$linear}[1 .. @$linear - 1] ) {
            $_->{'sql_alias'} = $collection->new_alias( $_->{'model'}, 'LEFT' );
            push @aliases, $_->{'sql_alias'};
        }

        foreach my $table ( @$linear ) {
            next unless $table->{'conditions'};
            my $ea = 'AND';
            $parser->walk( $table->{'conditions'}, {
                open_paren  => sub { $collection->open_paren('tisql-join', $_[1]) },
                close_paren => sub { $collection->close_paren('tisql-join', $_[1]) },
                operator    => sub { ${$_[3]} = $_[0] },
                operand     => sub {
                    my ($cond, $collection, $alias, $ea) = @_;
                    my %limit = (
                        subclause   => 'tisql-join',
                        leftjoin    => $alias,
                        entry_aggregator => $$ea,
                        alias       => $cond->{'lhs'}{'table'}{'sql_alias'},
                        column      => $cond->{'lhs'}{'column'},
                        operator    => $cond->{'op'},
                    );
                    if ( ref($cond->{'rhs'}) eq 'HASH' || ref($cond->{'rhs'}) eq 'Jifty::DBI::Tisql::Column' ) {
                        $limit{'quote_value'} = 0;
                        $limit{'value'} = $cond->{'rhs'}{'table'}{'sql_alias'} .'.'. $cond->{'rhs'}{'column'};
                    }
                    elsif ( $cond->{'rhs'} =~ /^%($re_ph_name)$/o ) {
                        my $phs = $joint->{'placeholders'}{ $1 };
                        return unless defined $phs;

                        if ( ref $phs ) {
                            $limit{'value'} = @$phs == 1? $phs->[0] : $phs;
                        }
                        elsif ( $phs eq '?' ) {
                            die "Not enough binding values provided for the query"
                                unless @{ $self->{'bindings'} };
                            $limit{'value'} = shift @{ $self->{'bindings'} };
                        }
                        else {
                            die "$phs is not supported placeholder argument";
                        }
                    }
                    elsif ( $cond->{'rhs'} eq '?' ) {
                        die "Can not use bindings ('?') in join condition";
                    }
                    else {
                        $limit{'value'} = $parser->dq( $cond->{'rhs'} );
                    }
                    $collection->limit( %limit );
                },
            }, $collection, $table->{'sql_alias'}, \$ea );
        }

        $last{'sql_alias'} = $joint->{'sql_alias'} = $linear->[-1]{'sql_alias'};
        $last{'item'}  = $linear->[-1]{'model'};
    }
    if ( !$args{'use_subjoin'} || @aliases < 2 ) {
        push @{ $collection->{'explicit_joins_order'} ||= [] }, @aliases;
    } else {
        push @{ $collection->{'explicit_joins_order'} ||= [] }, {
            type => 'LEFT',
            chain => \@aliases,
            criteria => delete $collection->{'joins'}{$aliases[0]}{'criteria'},
        };
        foreach ( @aliases[0, -1] ) {
            $collection->{'joins'}{ $_ }{'type'} = 'CROSS';
        }
    }
#    Test::More::diag( Dumper($meta, $collection, \@aliases) );

    return $last{'sql_alias'};
}

sub describe_join {
    my $self  = shift;
    my $model = shift;
    my $via   = shift;
    my $phs   = shift;

    $model = UNIVERSAL::isa( $model, 'Jifty::DBI::Collection' )
        ? $model->new_item
        : $model;

    my $column = $self->get_reference( $model => $via );

    my $refers_to = $column->refers_to->new;
    $refers_to = $refers_to->new_item
        if $refers_to->isa('Jifty::DBI::Collection');

    my $tree;
    if ( my $tisql = $column->tisql ) {
        my %props;
        if ( ref $tisql eq 'CODE' ) {
            %props = $tisql->( parser => $parser, tisql => $self, placeholders => $phs );
            die "If tisql join expression is a subroutine then it must return hash with 'query'"
                unless $props{'query'};
        } else {
            $props{'query'} = $tisql;
        }
        $tree = $parser->as_array( $props{'query'}, operand_cb => sub {
            return $self->parse_condition(
                'join', $_[0], sub { $self->parse_column( $_[0] ) }
            )
        } );
    } else {
        $tree = [ Q(
            C( $column->virtual? "id" : $via )
                => C( $via, '.', $column->by || 'id' )
        ) ];
    }
    my $res = {
        left  => $model,
        via   => $column,
        right => $refers_to,
        tree  => $tree,
    };
    return $res;
}

sub linearize_join {
    my $self  = shift;
    my $left         = shift;
    my $via          = shift;
    my $placeholders = shift;
    return $self->_linearize_join( $self->describe_join($left, $via, $placeholders) );
}

sub _linearize_join {
    my $self    = shift;
    my $join    = shift;
    my $inverse = shift;
    my $attach  = shift || {};
    my $placeholders = shift;

    my $inverse_on = $inverse? '' : $join->{'via'}->name; 

    my @res = (
        $attach->{'to'} || { model => $join->{'left'} },
        { model => $join->{'right'} },
    );
    my ($orig_left, $orig_right) = @res;
    @res = reverse @res if $inverse;

    my ($tree, $node, @pnodes);
    my %callback;
    $callback{'open_paren'} = sub {
        push @pnodes, $node;
        push @{ $pnodes[-1] }, $node = []
    };
    $callback{'close_paren'} = sub {
        $node = pop @pnodes;
        # filter out parens without conditions inside
        splice( @{ $node->[-1] }, -2 )
            unless @{ $node->[-1] };
    };
    $callback{'operator'}    = sub { push @$node, $_[0] };
    $callback{'operand'}     = sub {
        my $cond = $_[0];
        my %new_cond = %$cond;

        my $set_condition_on;

        foreach my $side (qw(lhs rhs)) {
            my ($left_border, $right_border) = (0, -1);
            unless ( ref $cond->{ $side } eq 'HASH' || ref $cond->{ $side } eq 'Jifty::DBI::Tisql::Column' ) {
                Carp::confess( "boo") if ref $cond->{ $side } eq 'HASH';

                if ( $placeholders && $cond->{ $side } =~ /^%($re_ph_name)$/o ) {
                    if ( defined ( my $phs = $placeholders->{ $1 } ) ) {
                        if ( ref $phs ) {
                            $new_cond{ $side } = @$phs == 1? $phs->[0] : $phs;
                        } else {
                            $new_cond{ $side } = $phs;
                        }
                    } else {
                        # we have to drop condition
                        pop @{ $node }; # delete operand
                        return;
                    }
                }
                next;
            }

            my $col = $cond->{ $side };
            my @chain = @{ $col->{'chain'} };

            unless ( @chain > 1 ) {
                # simple case
                $new_cond{ $side } = {
                    table  => $col->{'alias'}? $orig_right : $orig_left,
                    column => $col->{'chain'}[0]{'name'},
                };
                weaken($new_cond{ $side }{'table'});
                next;
            }

            my $last_column = pop @chain;

            my ($last_join, $conditions) = ( undef, [] );
            my $model = ($col->{'alias'}? $orig_right : $orig_left)->{'model'};
            foreach my $ref ( @chain ) {
                my $description = $self->describe_join( $model => $ref->{'name'}, $ref->{'placeholders'} );
                if ( $cond->{$side}{'alias'} eq $inverse_on ) {
                    my $linear = $self->_linearize_join(
                        $description, 'inverse', { to => $res[$right_border], place => $conditions }, $ref->{'placeholders'},
                    );
                    $last_join = $set_condition_on = $linear->[0];
                    splice @res, $right_border, 1, @$linear;
                    $right_border -= (@$linear - 1);
                } else {
                    my $linear = $self->_linearize_join(
                        $description, undef, { to => $res[$left_border] }, $ref->{'placeholders'},
                    );
                    $last_join = $linear->[-1];
                    splice @res, $left_border, 1, @$linear;
                    $left_border += (@$linear - 1);
                }

                $model = $self->get_reference( $model => $ref->{'name'} )->refers_to->new;
                $model = $model->new_item if $model->isa('Jifty::DBI::Collection');
            }
            push @$node, [ shift @$conditions, map +( 'AND' => $_ ), @$conditions ]
                if @$conditions;

            $new_cond{$side} = {
                table => $last_join,
                column => $last_column->{'name'},
            };
        }
        if ( $set_condition_on ) {
            $set_condition_on->{'conditions'} = [ \%new_cond ];
        } else {
            push @$node, \%new_cond;
        }
        return;
    };

    $tree = $node = [];
    $parser->walk( $join->{'tree'}, \%callback );

    if ( $attach->{'place'} ) {
        push @{ $attach->{'place'} }, $tree;
    } else {
        $res[-1]{'conditions'} = $tree;
    }
    return \@res;
}

sub check_query_condition {
    my ($self, $cond) = @_;

    die "Last column in '". $cond->{'lhs'} ."' is virtual" 
        if $cond->{'lhs'}{'column'}->virtual;

    if ( $cond->{'op_type'} eq 'col_op_col' ) {
        die "Last column in '". $cond->{'rhs'} ."' is virtual" 
            if $cond->{'rhs'}{'column'}->virtual;
    }

    return $cond;
}

sub parse_condition {
    my $self = shift;
    return Jifty::DBI::Tisql::Condition->parse(@_);
}

sub parse_column {
    my $self = shift;
    return Jifty::DBI::Tisql::Column->parse(@_);
}

sub external_reference {
    my $self = shift;
    my %args = @_;

    my $record = $args{'record'};
    my $column = $args{'column'};
    my $name   = $column->name;

    my $sql_alias = $self->{'collection'}->new_alias( $record );
    push @{ $self->{'collection'}{'explicit_joins_order'} ||= [] }, $sql_alias;

    my $aliases;
    local $self->{'aliases'} = $aliases = { __record__ => Jifty::DBI::Tisql::Column->new(
        alias        => '',
        is_qualified => 1,
        chain        => [ {
            name      => '__record__',
            refers_to => $record,
            sql_alias => $sql_alias,
        } ],
    ) };

    my $column_cb = sub {
        my $str = shift;
        $str = "__record__". $str if 0 == rindex $str, '.', 0;
        substr($str, 0, length($name)) = '' if 0 == rindex $str, "$name.", 0;
        return $self->parse_column($str)->qualify( tisql => $self, aliases => $aliases );
    };
    my $conditions = $parser->as_array(
        $column->tisql,
        operand_cb => sub {
            return $self->parse_condition( 'join', $_[0], $column_cb )
        },
    );
    $conditions = [
        $conditions,
        'AND',
        Jifty::DBI::Tisql::Condition->from_struct(
            $self->parse_column('__record__.id')->qualify( tisql => $self, aliases => $aliases ),
            $record->id || 0,
        ),
    ];
    $self->apply_query_tree( $conditions );

    return $self;
}

package Jifty::DBI::Tisql::Tree;

use overload
    '&'  => 'bit_and_op',
    '|'  => 'bit_or_op',
    '&=' => 'bit_assign_and_op',
    '|=' => 'bit_assign_or_op',

    '""' => "stringify",

    fallback => 1,
;

use Scalar::Util qw(blessed);

sub new {
    my $proto = shift;
    return (bless [], ref($proto)||$proto)->init(@_);
}

sub init {
    my $self = shift;
    @$self = @_;
    return $self;
}

sub bit_and_op { return (shift)->bit_op( @_, 'AND'); }
sub bit_or_op { return (shift)->bit_op( @_, 'OR'); }
sub bit_op {
    my ($self, $other, $invert, $op) = @_;

    die "$other is not a query condition" if $invert;

    die "'$other' is not a query condition"
        unless blessed $other
        && ( $other->isa('Jifty::DBI::Tisql::Tree')
            || $other->isa('Jifty::DBI::Tisql::Condition')
        );

    return $self->new( $self, $op, $other );
}

sub bit_assign_and_op { return (shift)->bit_assign_op( @_, 'AND') }
sub bit_assign_or_op  { return (shift)->bit_assign_op( @_, 'OR') }
sub bit_assign_op {
    my ($self, $other, $invert, $op) = @_;

    die "$other is not a query condition" if $invert;

    die "'$other' is not a query condition"
        unless blessed $other
        && ( $other->isa('Jifty::DBI::Tisql::Tree')
            || $other->isa('Jifty::DBI::Tisql::Condition')
        );

    return $self = $self->new( $self, $op, $other );
}

sub stringify {
    my $self = shift;
    return join ' ', '(', @$self, ')';
}

sub clone {
    my $self = shift;

    require Storable;

    my @new = @$self;
    foreach (@new) {
        if ( blessed $_ ) {
            $_ = $_->clone;
        } elsif ( ref $_ ) {
            $_ = Storable::dclone( $_ );
        }
    }
    return $self->new( @new );
}

package Jifty::DBI::Tisql::Condition;

use overload
    '&'  => 'bit_and_op',
    '|'  => 'bit_or_op',
    '&=' => 'bit_assign_and_op',
    '|=' => 'bit_assign_or_op',

    '""' => 'stringify',

    fallback => 1,
;

use Scalar::Util qw(blessed);

sub new {
    my $proto = shift;
    return (bless { }, ref($proto)||$proto)->init(@_);
}

sub init {
    my $self = shift;
    %$self = @_;
    return $self;
}

sub clone {
    my $self = shift;

    require Storable;

    my %new = %$self;
    foreach (qw(lhs rhs)) {
        if ( blessed $new{$_} ) {
            $new{$_} = $new{$_}->clone;
        } elsif ( ref $new{$_} ) {
            $new{$_} = Storable::dclone( $new{$_} );
        }
    }
    return $self->new( %new );
}

sub parse {
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
        if ( $string =~ /^(has(\s+no)?\s+)?($re_column)\s*($re_sql_op_bin)\s*($re_value|$re_binding)$/io ) {
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
        if ( $string =~ /^($re_column)\s*($re_sql_op_bin)\s*($re_value|$re_ph)$/io ) {
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
    return $self->new( %res );
}

sub from_struct {
    my $self = shift;
    my @args = @_;

    my %res = (
        type     => 'query',
        op_type  => undef,    # 'col_op', 'col_op_val' or 'col_op_col'
        modifier => '',       # '', 'has' or 'has no'
        lhs      => undef,
        op       => undef,
        rhs      => undef,
    );

    unless ( blessed $args[0] ) {
        if ( lc($args[0]) eq 'has' || lc($args[0]) eq 'has no' ) {
            $res{'modifier'} = lc shift @args;
            unless ( blessed $args[0] ) {
                $args[0] = Jifty::DBI::Tisql::Column->from_struct( $args[0] );
            }
        } else {
            $args[0] = Jifty::DBI::Tisql::Column->from_struct( $args[0] );
        }
    }

    if ( @args == 1 && $res{'modifier'} ) {
        @res{qw(op_type lhs op rhs)} = ('col_op', $args[0], $res{'modifier'} eq 'has'? 'IS NOT': 'IS', 'NULL');
        $res{'modifier'} = '';
        push @{ $res{'lhs'}{'chain'} }, { name => 'id', placeholders => {} };
    }
    elsif ( @args == 2 ) {
        if ( $args[1] =~ /^$re_sql_op_un$/i ) {
            @res{qw(op_type lhs op rhs)}
                = ('col_op', $args[0], split /\s*(?=null)/i, $args[1]);
        }
        else {
            # XXX: do we care about op_type?
            @res{qw(op_type lhs op rhs)}
                = ('col_op_val', $args[0], '=', $args[1]);
        }
    }
    elsif ( @args == 3 ) {
        die "$args[1] is not a valid operator"
            unless $args[1] =~ /^$re_sql_op_bin$/i;
        @res{qw(op_type lhs op rhs)}
            = ('col_op_col', $args[0], $args[1], $args[2]);
    }
    else {
        die "Invalid format";
    }
    return $self->new( %res );
}

sub bit_and_op { return (shift)->bit_op( @_, 'AND'); }
sub bit_or_op { return (shift)->bit_op( @_, 'OR'); }
sub bit_op {
    my ($self, $other, $invert, $op) = @_;

    die "$other is not a query condition" if $invert;

    die "'$other' is not a query condition"
        unless blessed $other
        && ( $other->isa('Jifty::DBI::Tisql::Tree')
            || $other->isa('Jifty::DBI::Tisql::Condition')
        );

    return Jifty::DBI::Tisql::Tree->new( $self, $op, $other );
}

sub bit_assign_and_op { return (shift)->bit_assign_op( @_, 'AND') }
sub bit_assign_or_op  { return (shift)->bit_assign_op( @_, 'OR') }
sub bit_assign_op {
    my ($self, $other, $invert, $op) = @_;

    die "$other is not a query condition" if $invert;

    die "'$other' is not a query condition"
        unless blessed $other
        && ( $other->isa('Jifty::DBI::Tisql::Tree')
            || $other->isa('Jifty::DBI::Tisql::Condition')
        );

    return $self = Jifty::DBI::Tisql::Tree->new( $self, $op, $other );
}

sub stringify {
    my $self = shift;
    return join ' ', grep defined && length, @{ $self }{'modifier', 'lhs', 'op', 'rhs'};
}

package Jifty::DBI::Tisql::Column;

use overload
    '""' => 'stringify',

    fallback => 1,
;

# represents something like:
# {
#   'alias'   => 'nodes',                            # alias or ''
#   'chain'   => [
#        {
#          'name' => 'attr',
#          'placeholders' => {name => 'category'},
#        },
#        {
#          'name' => 'value',
#          'placeholders' => {},
#        }
#   ],
# }
# no look ups, everything returned as is,

sub new {
    my $proto = shift;
    return (bless { }, ref($proto)||$proto)->init(@_);
}

sub init {
    my $self = shift;
    %$self = @_;
    return $self;
}

sub parse {
    my $self = shift;
    my $string = shift;

    my %res = (@_);

    my @columns;
    ($res{'alias'}, @columns) = split /\.($re_field$re_ph_access*)/o, $string;
    @columns = grep defined && length, @columns;

    my $prev = $res{'alias'};
    foreach my $col (@columns) {
        my $string = $col;
        $col =~ s/^($re_field)//;
        my $field = $1;

        my @phs = split /{\s*($re_ph_name)\s*=>\s*($re_cs_values|$re_ph|$re_binding)\s*}/, $col;
        @phs = grep !defined || length, @phs; # skip delimiters

        $col = {
            name => $field,
        };
        $col->{'placeholders'} = { @phs };
        foreach my $ph ( grep $_ ne '?' && !/^$re_ph$/o, values %{ $col->{'placeholders'} } ) {
            my @values;
            while ( $ph =~ s/^($re_value)\s*,?\s*// ) {
                push @values, $1;
            }
            $parser->dq( $_ ) foreach @values;
            $ph = \@values;
        }
    }
    $res{'chain'} = \@columns;

    return $self->new( %res );
}

sub from_struct {
    my $self = shift;
    my @args = @_;

    my %res = (alias => '');

    ($res{'alias'}) = splice @args, 0, 2
        if @args > 2 && $args[1] eq '.';

    my @columns;
    foreach my $element ( @args ) {
        unless ( ref $element ) {
            push @columns, { name => $element };
        }
        elsif ( ref $element eq 'HASH' ) {
            $columns[-1]{'placeholders'} = $element;
            foreach ( grep !ref $element->{ $_ }, keys %$element ) {
                $element->{ $_ } = [ $element->{ $_ } ];
            }
        }
        else {
            die "bla-bla wrong arguments";
        }
    }
    $res{'chain'} = \@columns;

    return $self->new( %res );
}

sub qualify {
    my $self = shift;
    my %args = (
        tisql      => undef,
        aliases    => undef,
        collection => undef,
        new        => 1,
        @_
    );
    return $self if $self->{'is_qualified'};

    my $tisql      = $args{'tisql'};
    my $aliases    = $args{'aliases'};
    my $collection = $args{'collection'} || $tisql->{'collection'};

    my $res = $args{'new'}? $self->clone( disqualify => 0 ) : $self;
    $res->{'is_qualified'} = 1;

    my $start_from = $res->{'alias'};
    my ($item, $last);
    if ( !$start_from && !$aliases->{''} ) {
        $item = $collection->new_item;
    } else {
        my $alias = $aliases->{ $start_from }
            || die "Couldn't find alias '$start_from'";

        $alias->qualify( tisql => $tisql, aliases => $aliases, collection => $collection, new => 0 );

        $last = $alias;
        $item = $alias->{'chain'}[-1]{'refers_to'};
        unless ( $item ) {
            die "last column in alias '$start_from' is not a reference";
        }
        $item = $item->new_item if $item->isa('Jifty::DBI::Collection');
    }

    my @chain = @{ $res->{'chain'} };
    while ( my $joint = shift @chain ) {
        my $name = $joint->{'name'};

        my $column = $tisql->get_reference( $item => $name );

        $joint->{'column'} = $column;
        $joint->{'on'} = $item;

        my $classname = $column->refers_to;
        if ( !$classname && @chain ) {
            die "column '$name' of ". ref($item) ." is not a reference"
                .", but used so in '$res'";
        }
        return $res unless $classname;

        if ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Collection' ) ) {
            $joint->{'refers_to'} = $classname->new( handle => $collection->_handle );
            $item = $joint->{'refers_to'}->new_item;
        }
        elsif ( UNIVERSAL::isa( $classname, 'Jifty::DBI::Record' ) ) {
            $joint->{'refers_to'} = $item = $classname->new( handle => $collection->_handle )
        }
        else {
            die "Column '$name' refers to '$classname' which is not record or collection";
        }
        $last = $joint;
    }

    return $res;
}

sub clone {
    my $self = shift;
    require Storable;
    my %args = (
        disqualify => 1,
        @_,
    );

    my $res = $self->new( %$self );
    $res->{'chain'} = Storable::dclone( $res->{'chain'} );
    if ( $args{'disqualify'} ) {
        delete @{ $_ }{'on', 'column', 'refers_to'} foreach @{ $res->{'chain'} };
        delete $res->{'is_qualified'};
    }
    return $res;
}

sub stringify {
    my $self = shift;
    my @parts = $self->{'alias'};
    foreach my $joint ( @{ $self->{'chain'} } ) {
        push @parts, $joint->{'name'};
        while ( my ($k, $v) = each %{ $joint->{'placeholders'} } ) {
            if ( ref $v ) {
                $parts[-1] .= "{ $k => ". join( ', ', map $parser->qq($_), @$v ) ." }";
            }
            else {
                $parts[-1] .= "{ $k => $v }";
            }
        }
    }
    return join '.', @parts;
}

1;
