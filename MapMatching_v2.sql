

drop function if exists FindSequ_core(integer,float8,float8,text);
begin;
/*Find the road segment according to given vector in the road network*/
create or replace function FindSequ_core(vid integer,dist float8, limitlen float8, edgetablename text)
returns text
as
$$
declare
sqlstr text;
rec record;
res text;
tmp text;
i integer;
begin
sqlstr:='select eid,endvt, startvt, cost from '|| edgetablename ||' where startvt = '||vid||' or endvt = '||vid;
res:= '';
i:=0;
for rec in execute sqlstr loop
i:=i+1;
res := res||'/'|| rec.eid ;
--raise notice 'id:%', res;
    if rec.endvt is null or rec.startvt is null then
        exit;
    end if;
    if dist+rec.cost < limitlen then
        res :=res ||'/'|| FindSequ_core(rec.endvt,(dist+rec.cost),limitlen, edgetablename);
	res :=res ||'/'|| FindSequ_core(rec.startvt,(dist+rec.cost),limitlen, edgetablename);
    end if;
end loop;
return res;
end;
$$
language 'plpgsql';
end;

/*ITERATION---Find the road segment according to given vector in the road network*/
drop function if exists FindSequ(integer,float,text);
begin;
create or replace function FindSequ(edgeid integer, peri float, edgetablename text)
returns setof record
as
$$
declare
sqlstr text;
limitlen float8;
rec record;
rec2 record;
timediffer float8;
res text;
res2 text;
begin

    sqlstr := 'select * from '||edgetablename||' where eid = '||edgeid||';';
    execute sqlstr into rec;
    limitlen := 80*1000/60*peri;
--raise notice 'rec.endvt:%, rec.cost:%, limitlen:%, timediffer:% ',rec.endvt, rec.cost, limitlen, timediffer;
    select FindSequ_core(rec.endvt, rec.cost, limitlen, edgetablename) into res;
    select FindSequ_core(rec.startvt, rec.cost, limitlen, edgetablename) into res2;
    res := res || res2;
    res := res ||'/'|| edgeid;
raise notice 'res:%',res;
  --     sqlstr := 'select * from regexp_split_to_table('||quote_literal('aa/bb/vv')||','||quote_literal('/')||')';
sqlstr := 'select eid as id from '|| edgetablename ||'limit 2';
return query select max(aa) from regexp_split_to_table(res,'/')  aa group by aa;
end;
$$
language 'plpgsql';
end;



/*Mapmatching function fitting for the road network*/
/*tolerance: the buffer allowence desided by the user; 
Allowtime: if the time interval is longer than given allowtime, the trajectory point is considered to be the start of the new trajectory*/
Drop function if exists MapMatching_v2(text,text,text,text,float,float) ;
begin;
create or replace function MapMatching_v2(roadtablename text,trajtablename text,
                                        restablename text,residuetablename text,tolerance float,allowtime float)
--returns setof record
returns boolean
as
$$
declare
sqlstr_1 text;
sqlstr_2 text;
sqlstr_3 text;
roadrec record;
pt record;
ptrec record;
respt text;
res text;
p1 geometry;
p2 geometry;
tmpLine geometry;
dist float;
x1 float;
y1 float;
x2 float;
y2 float;
roadx float;
roady float;
trajx float;
trajy float;
direct float;
srid text;
prevroad record;
prevpt record;
timestr interval;
isExec integer;
arc float;
sourcevt int;
targetvt int;
speed float8;
pathLen float8;
differ float8;
trajdist float8;
timediffer float8;
prevLoc float;
currLoc float;
tmpLoc float;
ntraj integer;
begin
sqlstr_1:='drop table if exists '||restablename||'; create table '||restablename||' as select * from '
    ||trajtablename||' where 1=0 ;alter table '||restablename||' add roadid int;';
execute sqlstr_1;
sqlstr_1:='drop table if exists '||residuetablename||'; create table '||residuetablename||' as select * from '
    ||trajtablename||' where 1=0 ; alter table '||residuetablename||' add reason varchar;';
execute sqlstr_1;
sqlstr_1:='select st_srid(geom) from '||trajtablename||' limit 1';
execute sqlstr_1 into srid;
sqlstr_1:='create temp table tmptb as select * from '||restablename||' where 1=0';
execute sqlstr_1;
/*Match every point in the trajectory*/
sqlstr_1:='select *,st_geomfromtext((select st_astext(cast(st_expand(geom,'||tolerance||') as box2d))),'||srid||') as buffer 
from '||trajtablename||' order by id;';
--timestr:=quote_literal(allowtime)||' minute';
ntraj:=0;
for ptrec in execute sqlstr_1 
loop
 ntraj := ntraj+1;

/*Find the road segment near the trajectory point within given distance*/
    sqlstr_2:='select eid, geom, st_distance(st_geomfromtext('||quote_literal((select st_astext(ptrec.geom)))||','||srid||'), geom) as dist from '||roadtablename||' 
 where st_intersects(geom,st_geomfromtext(('||quote_literal((select st_astext(ptrec.buffer)))||'),'||srid||')) order by dist';

    for roadrec in execute sqlstr_2 loop
        if ptrec.id = 1 then
            prevpt = ptrec;
        end if;
        if ptrec.id=1 or (ptrec.datebynum-prevpt.datebynum) *24*60> allowtime then
/* For the first trajectory, directely match it to the nearest road segment
*/

                insert into tmptb(id,geom,longitude,latitude,altitude, datebynum, pdate, ptime,roadid) 
values(ptrec.id,(st_closestpoint(roadrec.geom,ptrec.geom)),ptrec.longitude,ptrec.latitude,ptrec.altitude, ptrec.datebynum, ptrec.pdate, ptrec.ptime,roadrec.eid);
                prevroad:=roadrec;
                isExec:=1;
                exit;
        end if;
/* For the not first trajectory, match it according to the previous segment that been matched
*/
        if (cast(roadrec.eid as text) in (select a.a from FindSequ(prevroad.eid,((ptrec.datebynum-prevpt.datebynum)*24*60), roadtablename) a(a text))) or (roadrec.eid = prevroad.eid) then
            insert into tmptb(id,geom,longitude,latitude,altitude, datebynum, pdate, ptime,roadid) 
values(ptrec.id,(st_closestpoint(roadrec.geom,ptrec.geom)), ptrec.longitude,ptrec.latitude,ptrec.altitude, ptrec.datebynum, ptrec.pdate, ptrec.ptime,roadrec.eid);
                prevroad:=roadrec;
                isExec:=1;
                exit;
        end if;

    end loop;
    if isExec!=1 and (roadrec.eid is not null) then
        select st_astext(ptrec.geom) into respt;
        sqlstr_3:='insert into '||residuetablename||'(id,geom,longitude,latitude,altitude, datebynum, pdate, ptime,reason) 
values('||ptrec.id||',st_geomfromtext('||quote_literal(respt::text)||','||srid||')::geometry,'||ptrec.longitude||','||quote_literal(ptrec.latitude)||',
'||ptrec.altitude||','||ptrec.datebynum||','||quote_literal(ptrec.pdate::text)||','||quote_literal(ptrec.ptime::text)||','||quote_literal('NotFit')||');';
        --prevroad:=roadrec;
        execute sqlstr_3;
    end if;
    if roadrec.eid is null then
            select st_astext(ptrec.geom) into respt;
            sqlstr_3:='insert into '||residuetablename||'(id,geom,longitude,latitude,altitude, datebynum, pdate, ptime,reason) 
values('||ptrec.id||',st_geomfromtext('||quote_literal(respt::text)||','||srid||'),'||ptrec.longitude||','||quote_literal(ptrec.latitude)||',
'||ptrec.altitude||','||ptrec.datebynum||','||quote_literal(ptrec.pdate::text)||','||quote_literal(ptrec.ptime::text)||','||quote_literal('outOfRange')||');';
            execute sqlstr_3;
            ptrec:=prevpt;
    end if; 
isExec:=0;
    prevpt:=ptrec;
end loop;
sqlstr_1:='insert into '||restablename||' select * from tmptb';
execute sqlstr_1;
drop table tmptb;
return true;
end;
$$
language 'plpgsql';
end;




/*select a.a,a.c from  
mapmatching('nanjing_road','traj','resulttable',10,100) 
a(a integer,b geometry,c float);*/
--select  MapMatching_v2('edgetable','sample_806401925019','v2_result_sample_806401925019','v2_residue_sample_806401925019',0.00015,1.5)
--select FindSequ_core(1046, 150, 800);
--select a.a from FindSequ(917,'30 seconds') a(a text);

