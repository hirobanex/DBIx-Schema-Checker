package DBIx::Schema::Checker::DBIxSkinny;
use strict;
use warnings;
use Sub::Args;
use Text::Diff qw/diff/;
use DBIx::Inspector;

our @EXPORT = qw/schema_check/;
use Exporter 'import';

sub schema_check{
    my (%args) = @_;

    my $self = Schema::Checker->new(%args);

    $self->diff_production;
    $self->diff_skinny;
}

sub new{
    my $class = shift;
   
    my $self = bless {
        skinny_schema_info => '',
        dbh                => {
            staging    => '',
            production => '',
        },
        tables             => {
            staging    => {},
            production => {},
            skinny     => {},
        },
        diff               => {
            production => '',
            skinny     => '',
        },
        @_
    },$class;

    $self->_set_tables;

    return $self;
}


sub diff_skinny{
    my ($self) = @_;
    
    $self->_diff_tables('skinny');
    
    my $inspector = DBIx::Inspector->new(dbh => $self->{dbh}->{staging});
    
    for my $table($inspector->tables){
        my $table_name  = $table->name;
        my @pks = $table->primary_key;
        my $pk  = shift @pks;
        my $pk_name = $pk ? $pk->name: '';

        $self->{diff}->{skinny} .= diff(
            \($table_name.':'.$pk_name),
            \($table_name.':'.$self->{skinny_schema_info}->{$table_name}->{pk})
        )."\n";

        $self->{diff}->{skinny} .= diff(
            [sort map {$_->name} $table->columns],
            [sort @{$self->{skinny_schema_info}->{$table_name}->{columns}}]
        )."\n";
    }

    $self->_error_check('staging schema skinny schema diff is ','skinny');
}

sub diff_production{
    my ($self) = @_;
    
    $self->_diff_tables('production');
    
    for my $table(keys %{$self->{tables}->{staging}}){
        my $create_table = {};
        for my $database('staging','production'){
            $create_table->{$database} = $self->{dbh}->{$database}->selectrow_hashref("show create table $table");
            $create_table->{$database}->{'Create Table'} =~ s/AUTO_INCREMENT\=\d+\s//smx;
        }
        
        $self->{diff}->{production} .= diff(
            \($table.':'.$create_table->{staging}->{'Create Table'}),
            \($table.':'.$create_table->{production}->{'Create Table'})
        )."\n";
    }
    
    $self->_error_check('staging database production database diff is ','production');
}

sub _error_check{
    my ($self,$error_message,$type) = @_;
    
    (my $diff_production = $self->{diff}->{$type}) =~ s/\n//g;

    if($diff_production){
        die $error_message.":\n".$self->{diff}->{$type};
    }
}

sub _diff_tables{
    my ($self,$checked) = @_;
    
    $self->_diff_tables_checker($checked,'staging');
    $self->_diff_tables_checker('staging',$checked);

}

sub _diff_tables_checker{
    my ($self,$before,$after) = @_;

    map {
        unless(exists $self->{tables}->{$before}->{$_}){
            die "$before db does not have table_name: $_.";
        }
    } keys %{$self->{tables}->{$after}};
}

sub _set_tables{
    my ($self) = @_;
    
    for my $database(keys %{$self->{dbh}}){
        map {
            $self->{tables}->{$database}->{shift @{$_}} = 1
        } @{$self->{dbh}->{$database}->selectall_arrayref('show tables')}
    }

    map {$self->{tables}->{skinny}->{$_} = 1} keys %{$self->{skinny_schema_info}};
}
