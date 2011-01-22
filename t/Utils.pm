package t::Utils;
use strict;
use warnings;
use utf8;
use lib './t/lib';
use Test::More;
use Data::Section::Simple qw(get_data_section);
use DBI;

sub import {
    strict->import;
    warnings->import;
    utf8->import;
}

sub create_mysql_db{
    my ($staging,$production) = @_;

    my $dbh = DBI->connect("dbi:mysql:;mysql_multi_statements=1", "user", "pass");

    my $sqls = get_data_section;

    for my $db_name($staging,$production){
        $dbh->do("CREATE DATABASE $db_name");

        my $sql = "USE $db_name;\n";
        $sql   .= "SET NAMES utf8;\n";
        $sql   .= $sqls->{$db_name};

        $dbh->do($sql);
    }
}

sub get_dbh{
    my $db_name = shift;

    my $dbh = DBI->connect("dbi:mysql:$db_name;", "user", "pass");
}

1;

__DATA__

@@ staging
CREATE TABLE user(
    id          int(10) unsigned    NOT NULL auto_increment,
    name        varchar(255) binary NOT NULL,
    status      tinyint(1)          NOT NULL default 0,

    PRIMARY KEY     (id),
    UNIQUE  uniq    (name),
    KEY     status  (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT 'testging table';

@@ production
CREATE TABLE user(
    id          int(10) unsigned    NOT NULL auto_increment,
    name        varchar(255) binary NOT NULL,
    status       tinyint(1)         NOT NULL default 0,

    PRIMARY KEY     (id),
    UNIQUE  uniq    (name),
    KEY     status  (status)
) ENGINE=InnoDB AUTO_INCREMENT=9226 DEFAULT CHARSET=utf8 COMMENT 'testging table';
