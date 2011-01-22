use t::Utils;
use DBIx::Schema::Checker::DBD::mysql;
use Test::More;

t::Utils::create_mysql_db('staging','production');

my $check_result = schema_check(
    staging_dbh    => t::Utils::get_dbh('staging'),
    production_dbh => t::Utils::get_dbh('production'),
);

is $check_result,0;

done_testing;
