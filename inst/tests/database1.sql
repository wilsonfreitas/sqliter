
drop table if exists test1;
drop table if exists test2;

create table test1 (c1, c2);
create table test2 (c1, c2);

insert into test1 values (1, 'a');
insert into test1 values (2, 'b');
insert into test1 values (3, 'c');
insert into test1 values (4, 'd');

select * from test1;

