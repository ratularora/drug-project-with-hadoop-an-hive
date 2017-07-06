drop database IF EXISTS month CASCADE ;
create database month;
use month;

create table drug(chemical string,dates string,age1 float ,age2 float,age3 float,age4 float,age5 float,age6 float,age7 float,age8 float,age9 float) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile;
describe drug;
load data local inpath '/home/ratul/drug/full' into table drug;
select chemical,dates,age3 from drug where month(dates)="09" ;




