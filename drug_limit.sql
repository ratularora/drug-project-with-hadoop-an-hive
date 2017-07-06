drop database IF EXISTS chemical_name CASCADE;
create database chemical_name;
use chemical_name;

create table drug(chemical string,dates string,age1 float ,age2 float,age3 float,age4 float,age5 float,age6 float,age7 float,age8 float,age9 float) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
describe drug;
load data local inpath '/home/ratul/drug/result_chemical' into table drug;
insert overwrite directory '/user/result11' row format delimited fields terminated by ' , ' select * from drug limit 30 ;

