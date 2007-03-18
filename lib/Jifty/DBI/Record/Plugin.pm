package Jifty::DBI::Record::Plugin;

use warnings;
use strict;

use base qw/Exporter/;

=head1 NAME

Jifty::DBI::Record::Plugin - Record model mixins for Jifty::DBI

=head1 SYNOPSIS

  # Define a mixin
  package MyApp::FavoriteColor;
  use base qw/ Jifty::DBI::Record::Plugin /;

  use Jifty::DBI::Schema;
  use Jifty::DBI::Record schema {
      column favorite_color =>
          type is 'text',
          label is 'Favorite Color',
          valid_values are qw/ red green blue yellow /;
  };

  # Use the mixin
  package MyApp::Model::User;

  use Jifty::DBI::Schema;
  use Jifty::DBI::Record schema {
      column name =>
          type is 'text',
          label is 'Name';
  };

  # Mixins
  use MyApp::FavoriteColor;

  sub name_and_color {
      my $self  = shift;
      my $name  = $self->name;
      my $color = $self->favorite_color;

      return "The favorite color of $name is $color.";
  }

=head1 DESCRIPTION

By using this package you may provide models that are built from one or more mixins. In fact, your whole table could be defined in the mixins without a single column declared within the model class itself.

=head2 MODEL MIXINS

To build a mixin, just create a model that inherits from this package, C<Jifty::DBI::Record::Plugin>. Then, add the schema definitions you want inherited.

  package MyApp::FasterSwallow;
  use base qw/ Jifty::DBI::Record::Plugin /;
  
  use Jifty::DBI::Schema;
  use Jifty::DBI::Record schema {
      column swallow_type =>
          type is 'text',
          valid are qw/ african european /,
          default is 'african';
  };

=head3 register_triggers

Your mixin may also want to register triggers for the records to which it will be added. You can do this by defining a method named C<register_triggers>:

  sub register_triggers {
      my $self = shift;
      $self->add_trigger( 
          name      => 'before_create', 
          callback  => \&before_create,
          abortable => 1,
      );
  }

  sub before_create {
      # do something...
  }

See L<Jifty::DBI::Class::Trigger>.

=head2 MODELS USING MIXINS

To use your model plugin, just use the mixins you want to get columns from. You should still include a schema definition, even if it's empty:

  package MyApp::Model::User;

  use Jifty::DBI::Schema;
  use MyApp::Record schema {
  };

  # Mixins
  use MyApp::FavoriteColor;
  use MyApp::FasterSwallow;
  use Jifty::Plugin::User::Mixin::Model::User;
  use Jifty::Plugin::Authentication::Password::Mixin::Model::User;

=cut

sub import {
    my $self = shift;
    my $caller = caller;
    for ($self->columns) {
            $caller->COLUMNS->{$_->name} = $_ ;
            $caller->_init_methods_for_column($_);
    }
    $self->export_to_level(1,undef);
    
    if (my $triggers =  $self->can('register_triggers') ) {
        $triggers->($caller)
    }
}

=head1 SEE ALSO

L<Jifty::DBI::Record>, L<Jifty::DBI::Class::Trigger>

=head1 LICENSE

Jifty::DBI is Copyright 2005-2007 Best Practical Solutions, LLC.
Jifty is distributed under the same terms as Perl itself.

=cut

1;
