package DBIx::Schema::Checker::DBD::mysql;
use strict;
use warnings;
use Sub::Args;
use Text::Diff qw/diff/;
our @EXPORT = qw/schema_check/;
use Exporter 'import';

sub schema_check{
    my $args = args({
        staging_dbh    => 1,
        production_dbh => 1,
    },@_);

    my $self = DBIx::Schema::Checker::DBD::mysql->new($args);

    $self->diff_schema;
}

#どうせSkinny、Mysqlだけだし、ネーミングがね
#rowのありなしもチェックしようかな
#productionが複数あってもいい気がする

sub new{
    my ($class,$args) = @_;
   
    my $self = bless {
        staging    => {
            dbh     => $args->{staging_dbh},
            _tables => [],
        },
        production => {
            dbh     => $args->{production_dbh},
            _tables => [],
        }
    },$class;

    $self->_set_tables;

    return $self;
}

sub diff_schema{
    my ($self) = @_;
    
    $self->_diff_tables;
    
    my $diff = {};
    for my $table(keys %{$self->{staging}->{_tables}}){

        my $create_table = {};
    
        for my $database('staging','production'){
            $create_table->{$database} = $self->{$database}->{dbh}->selectrow_hashref("SHOW CREATE TABLE  $table");
            $create_table->{$database}->{'Create Table'} =~ s/AUTO_INCREMENT\=\d+\s//smx;
        }
        
        $diff->{$table} = diff(
            \($create_table->{staging}->{'Create Table'}),\($create_table->{production}->{'Create Table'}),
            FILENAME_A => "staging $table"               , FILENAME_B => "production $table"
        );
        
        if($diff->{$table}){
            die $diff->{$table};
        }
    }

    return 0;
}

sub _diff_tables{
    my ($self) = @_;
    
    unless(@{$self->{staging}->{_tables}} == @{$self->{production}->{_tables}}){
        die 'table count is not same';        
    }

    for(my $i = 0; $i < @{$self->{staging}->{_tables}}; $i++){
        unless($self->{staging}->{_tables}->[$i] eq $self->{production}->{_tables}->[$i]){
            die 'table count is not same :'.
                $self->{staging}->{_tables}->[$i].':'.
                $self->{production}->{_tables}->[$i].';'
            ;        
        }
    }
}

sub _set_tables{
    my ($self) = @_;

    for my $database('staging','production'){
        $self->{$database}->{_tables} = [sort map {$_->[0]} @{$self->{dbh}->{$database}->selectall_arrayref('show tables')}];
    }
}
