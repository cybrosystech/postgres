--
-- DBBLUE_FK_JOIN_REDUCTION
--
-- Reducing an outer join to an inner join when a mandatory foreign key proves
-- that every preserved-side row has exactly one match on the nullable side.
--
-- Each case checks two things: whether the reduction fired, and that the outer
-- join still returns exactly what an outer join must.  A reduction that fires
-- when it may not shows up as a row-count difference against the inner join.
--

-- Report whether the top join was reduced, without depending on plan shape.
create function fk_plan_kind(q text) returns text language plpgsql as $$
declare ln text;
begin
  for ln in execute 'explain (costs off) ' || q loop
    if ln ~ 'Full Join' then return 'FULL (not reduced)'; end if;
    if ln ~ 'Left Join' then return 'LEFT (not reduced)'; end if;
    if ln ~ 'Right Join' then return 'RIGHT (not reduced)'; end if;
    if ln ~ '(Hash|Merge|Nested Loop)' then return 'INNER (reduced)'; end if;
  end loop;
  return 'no join';
end $$;

create table fkr_p (id int primary key, v int);
create table fkr_c (id int primary key, pid int not null references fkr_p(id),
                    tag text);
insert into fkr_p select g, g * 10 from generate_series(1, 20) g;
insert into fkr_c select g, 1 + (g % 20), 't' || g from generate_series(1, 60) g;
analyze fkr_p, fkr_c;

-- mandatory FK: reduces, and the two join types agree
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id') as plan,
       (select count(*) from fkr_c c left join fkr_p p on c.pid = p.id) as outer_rows,
       (select count(*) from fkr_c c join fkr_p p on c.pid = p.id) as inner_rows;

-- the GUC turns it off
set dbblue_enable_fk_join_reduction = off;
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id') as plan;
reset dbblue_enable_fk_join_reduction;

-- RIGHT JOIN is the mirror image and also reduces
select fk_plan_kind('select c.id, p.v from fkr_p p right join fkr_c c on c.pid = p.id') as plan,
       (select count(*) from fkr_p p right join fkr_c c on c.pid = p.id) as outer_rows,
       (select count(*) from fkr_p p join fkr_c c on c.pid = p.id) as inner_rows;

-- FULL JOIN cannot be reduced: both sides are nullable
select fk_plan_kind('select c.id, p.v from fkr_c c full join fkr_p p on c.pid = p.id') as plan;

-- a nullable FK column proves nothing: a NULL satisfies MATCH SIMPLE with no
-- referenced row present
alter table fkr_c alter column pid drop not null;
insert into fkr_c values (9001, null, 'null-fk');
analyze fkr_c;
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id') as plan,
       (select count(*) from fkr_c c left join fkr_p p on c.pid = p.id) as outer_rows,
       (select count(*) from fkr_c c join fkr_p p on c.pid = p.id) as inner_rows;
delete from fkr_c where id = 9001;
alter table fkr_c alter column pid set not null;

-- no FK at all
alter table fkr_c drop constraint fkr_c_pid_fkey;
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id') as plan;

-- NOT VALID FK proves nothing, and an orphan row can actually exist
insert into fkr_c values (9002, 999, 'orphan');
alter table fkr_c add constraint fkr_c_pid_fkey foreign key (pid)
  references fkr_p(id) not valid;
analyze fkr_c;
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id') as plan,
       (select count(*) from fkr_c c left join fkr_p p on c.pid = p.id) as outer_rows,
       (select count(*) from fkr_c c join fkr_p p on c.pid = p.id) as inner_rows;
alter table fkr_c drop constraint fkr_c_pid_fkey;
delete from fkr_c where id = 9002;

-- a DEFERRABLE FK may legally be violated in mid-transaction, which is exactly
-- where the two join types diverge, so it must not be trusted
alter table fkr_c add constraint fkr_c_pid_fkey foreign key (pid)
  references fkr_p(id) deferrable initially deferred;
analyze fkr_c;
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id') as plan;
begin;
  insert into fkr_c values (9003, 4242, 'deferred-orphan');
  select count(*) as outer_rows from fkr_c c left join fkr_p p on c.pid = p.id;
  select count(*) as inner_rows from fkr_c c join fkr_p p on c.pid = p.id;
rollback;
alter table fkr_c drop constraint fkr_c_pid_fkey;
alter table fkr_c add constraint fkr_c_pid_fkey foreign key (pid)
  references fkr_p(id);

-- any extra ON qual can reject the matching row, on either side
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id and p.v > 100') as plan,
       (select count(*) from fkr_c c left join fkr_p p on c.pid = p.id and p.v > 100) as outer_rows,
       (select count(*) from fkr_c c join fkr_p p on c.pid = p.id and p.v > 100) as inner_rows;
select fk_plan_kind($$select c.id, p.v from fkr_c c left join fkr_p p on c.pid = p.id and c.tag = 't5'$$) as plan,
       (select count(*) from fkr_c c left join fkr_p p on c.pid = p.id and c.tag = 't5') as outer_rows,
       (select count(*) from fkr_c c join fkr_p p on c.pid = p.id and c.tag = 't5') as inner_rows;

-- the operator must be the FK's own equality operator
select fk_plan_kind('select c.id, p.v from fkr_c c left join fkr_p p on c.pid >= p.id') as plan;

-- the referencing side must not be nullable from a lower outer join, or its
-- NOT NULL column can still read as NULL
create table fkr_top (id int primary key, cid int);
insert into fkr_top select g, case when g % 2 = 0 then g end
  from generate_series(1, 20) g;
analyze fkr_top;
select fk_plan_kind('select t.id, p.v from fkr_top t left join fkr_c c on t.cid = c.id left join fkr_p p on c.pid = p.id') as plan,
       (select count(*) from fkr_top t left join fkr_c c on t.cid = c.id left join fkr_p p on c.pid = p.id) as outer_rows,
       (select count(*) from fkr_top t left join fkr_c c on t.cid = c.id join fkr_p p on c.pid = p.id) as inner_rows;

-- composite foreign keys reduce when every column is covered ...
create table fkr_pp (a int, b int, v int, primary key (a, b));
create table fkr_cc (id int primary key, a int not null, b int not null,
                     foreign key (a, b) references fkr_pp(a, b));
insert into fkr_pp select g, g + 1, g * 7 from generate_series(1, 10) g;
insert into fkr_cc select g, 1 + (g % 10), 2 + (g % 10) from generate_series(1, 40) g;
analyze fkr_pp, fkr_cc;
select fk_plan_kind('select cc.id, pp.v from fkr_cc cc left join fkr_pp pp on cc.a = pp.a and cc.b = pp.b') as plan,
       (select count(*) from fkr_cc cc left join fkr_pp pp on cc.a = pp.a and cc.b = pp.b) as outer_rows,
       (select count(*) from fkr_cc cc join fkr_pp pp on cc.a = pp.a and cc.b = pp.b) as inner_rows;

-- ... but not when the ON clause covers only part of the key
select fk_plan_kind('select cc.id, pp.v from fkr_cc cc left join fkr_pp pp on cc.a = pp.a') as plan;

drop table fkr_cc, fkr_pp, fkr_top, fkr_c, fkr_p;
drop function fk_plan_kind(text);
