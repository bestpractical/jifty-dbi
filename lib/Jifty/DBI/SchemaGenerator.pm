use strict;
use warnings;

package Jifty::DBI::SchemaGenerator;

use base qw(Class::Accessor);
use DBIx::DBSchema;
use Class::ReturnValue;

our $VERSION = '0.01';

# Public accessors
__PACKAGE__->mk_accessors(qw(handle));

# Internal accessors: do not use from outside class
__PACKAGE__->mk_accessors(qw(_db_schema));

=head1 NAME

Jifty::DBI::SchemaGenerator - Generate table schemas from Jifty::DBI records

=head1 SYNOPSIS

    use Jifty::DBI::SchemaGenerator;

=head1 DESCRIPTION

This module turns a Jifty::Record object into an SQL schema for your chosen
database. At the moment, your chosen database is Postgres 7.4 or newer.
Patches welcome.

=head1 INTERFACE 

=for author to fill in:
    Write a separate section listing the public components of the modules
    interface. These normally consist of either subroutines that may be
    exported, or methods that may be called on objects belonging to the
    classes provided by the module.


=head1 DIAGNOSTICS

=for author to fill in:
    List every single error and warning message that the module can
    generate (even the ones that will "never happen"), with a full
    explanation of each problem, one or more likely causes, and any
    suggested remedies.

=over

=item C<< Error message here, perhaps with %s placeholders >>

[Description of error here]

=item C<< Another error message here >>

[Description of error here]

[Et cetera, et cetera]

=back


=head1 CONFIGURATION AND ENVIRONMENT

=for author to fill in:
    A full explanation of any configuration system(s) used by the
    module, including the names and locations of any configuration
    files, and the meaning of any environment variables or properties
    that can be set. These descriptions must also include details of any
    configuration language used.

<MODULE NAME> requires no configuration files or environment variables.


=head1 DEPENDENCIES

=for author to fill in:
    A list of all the other modules that this module relies upon,
    including any restrictions on versions, and an indication whether
    the module is part of the standard Perl distribution, part of the
    module's distribution, or must be installed separately. ]

None.

=head2 new HANDLE

Creates a new C<Jifty::DBI::SchemaGenerator> object.  The single
required argument is a C<Jifty::DBI::Handle>.

=cut

sub new {
    my $class  = shift;
    my $handle = shift;
    my $self   = $class->SUPER::new();

    $self->handle($handle);

    my $schema = DBIx::DBSchema->new();
    $self->_db_schema($schema);

    return $self;
}

=for public_doc add_model MODEL

Adds a new model class to the SchemaGenerator.  Model should either be an object 
of a subclass of C<Jifty::DBI::Record>, or the name of such a subclass; in the
latter case, C<add_model> will instantiate an object of the subclass.

The model must define the instance methods C<Schema> and C<Table>.

Returns true if the model was added successfully; returns a false C<Class::ReturnValue> error
otherwise.

=cut

sub add_model {
    my $self  = shift;
    my $model = shift;

    # $model could either be a (presumably unfilled) object of a subclass of
    # Jifty::DBI::Record, or it could be the name of such a subclass.

    unless ( ref $model and UNIVERSAL::isa( $model, 'Jifty::DBI::Record' ) ) {
        my $new_model;
        eval { $new_model = $model->new; };

        if ($@) {
            return $self->_error("Error making new object from $model: $@");
        }

        unless ( UNIVERSAL::isa( $new_model, 'Jifty::DBI::Record' ) ) {
            return $self->_error(
                "Didn't get a Jifty::DBI::Record from $model, got $new_model"
            );
        }
        $model = $new_model;
    }

    my $table_obj = $self->_db_schema_table_from_model($model);

    $self->_db_schema->addtable($table_obj);

    1;
}

=for public_doc create_table_sql_statements

Returns a list of SQL statements (as strings) to create tables for all of
the models added to the SchemaGenerator.

=cut

sub create_table_sql_statements {
    my $self = shift;

    # The sort here is to make it predictable, so that we can write tests.
    return sort $self->_db_schema->sql( $self->handle->dbh );
}

=for public_doc create_table_sql_text

Returns a string containg a sequence of SQL statements to create tables for all of
the models added to the SchemaGenerator.

=cut

sub create_table_sql_text {
    my $self = shift;

    return join "\n", map {"$_ ;\n"} $self->create_table_sql_statements;
}

=for private_doc _db_schema_table_from_model MODEL

Takes an object of a subclass of Jifty::DBI::Record; returns a new
C<DBIx::DBSchema::Table> object corresponding to the model.

=cut

sub _db_schema_table_from_model {
    my $self  = shift;
    my $model = shift;

    my $table_name = $model->table;
    my @columns    = $model->columns;

    my @cols;

    for my $column (@columns) {

        # Skip "Virtual" columns - (foreign keys to collections)
        next if $column->virtual;
        next if defined $column->alias_for_column;

        push @cols,
            DBIx::DBSchema::Column->new(
            {   name     => $column->name,
                type     => $column->type,
                null     => $column->mandatory ? 0 : 1,
                default  => $column->default,
            }
            );
    }

    my $table = DBIx::DBSchema::Table->new(
        {   name        => $table_name,
            primary_key => "id",
            columns     => \@cols,
        }
    );

    return $table;
}

=for private_doc _error STRING

Takes in a string and returns it as a Class::ReturnValue error object.

=cut

sub _error {
    my $self    = shift;
    my $message = shift;

    my $ret = Class::ReturnValue->new;
    $ret->as_error( errno => 1, message => $message );
    return $ret->return_value;
}

1;    # Magic true value required at end of module

=head1 INCOMPATIBILITIES

=for author to fill in:
    A list of any modules that this module cannot be used in conjunction
    with. This may be due to name conflicts in the interface, or
    competition for system or program resources, or due to internal
    limitations of Perl (for example, many modules that use source code
    filters are mutually incompatible).

None reported.


=head1 BUGS AND LIMITATIONS

=for author to fill in:
    A list of known problems with the module, together with some
    indication Whether they are likely to be fixed in an upcoming
    release. Also a list of restrictions on the features the module
    does provide: data types that cannot be handled, performance issues
    and the circumstances in which they may arise, practical
    limitations on the size of data sets, special cases that are not
    (yet) handled, etc.

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-<RT NAME>@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.


=head1 AUTHOR

David Glasser  C<< glasser@bestpractical.com >>


=head1 LICENCE AND COPYRIGHT

Copyright (c) 2005, Best Practical Solutions, LLC.  All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.


=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

=cut

