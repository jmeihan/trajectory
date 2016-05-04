<?php


   /*$sqlstr_1 = " select st_astext((st_accum(st_geomfromtext('".$row[1]."',4326)))[1]) as geom ";
    $result_1 = pg_query($conn, $sqlstr_1);
    $Geom = pg_fetch_result($result_1, 0, "geom");
    if(!$Geom ){
        echo $Geom."\n";
        $Geom = $row[1];
        
    }*/
function addGeomArr($geomArr, $roadedge, $blade, $conn){
    $sqlstr = "select st_astext((st_dump(st_split(st_geomfromtext('".$roadedge."',4326), st_geomfromtext('".$blade."',4326)))).geom) as geom";
    $result = pg_query($conn, $sqlstr);
    while ($row = pg_fetch_row($result)) {
        array_push($geomArr, $row[0]);
    }
    return $geomArr;
}

function splitRecur($geomArr, $blade, $conn, $edgetb){
    foreach ($geomArr as $geom) {
       $sqlstr_sub = "select (st_crosses('$geom'::geometry, '$blade'::geometry) or st_touches('$geom'::geometry, '$blade'::geometry)) 
        and (((st_equals(st_startpoint('$blade'::geometry), st_startpoint('$geom'::geometry))) IS FALSE) and ((st_equals(st_endpoint('$blade'::geometry), st_startpoint('$geom'::geometry))) IS FALSE)
        and ((st_equals(st_startpoint('$blade'::geometry), st_endpoint('$geom'::geometry))) IS FALSE) and ((st_equals(st_endpoint('$blade'::geometry), st_endpoint('$geom'::geometry))) IS FALSE)) 
        and (st_within(st_startpoint('$geom'::geometry), '$blade'::geometry) IS FALSE) and (st_within(st_endpoint('$geom'::geometry), '$blade'::geometry) IS FALSE) ;";
        /* 
        $sqlstr_sub = "select (st_crosses('$geom'::geometry, '$blade'::geometry) or  st_touches('$geom'::geometry, '$blade'::geometry)) 
        and (st_within(st_startpoint('$geom'::geometry), '$blade'::geometry) IS FALSE) and (st_within(st_endpoint('$geom'::geometry), '$blade'::geometry) IS FALSE)";*/
        $result_sub = pg_query($conn,$sqlstr_sub);
        if(!$result_sub)
        {
            echo pg_last_error();
            pg_close($conn);
            exit(0);        
        }
        //echo $sqlstr_sub."\n";
    if(pg_num_rows($result_sub) == 0){
        break;
    }

        $row_sub = pg_fetch_row($result_sub);
        echo $row_sub[0];
        if($row_sub[0] == 'f'){
            break;
        }else{
        
            $geomArr = addGeomArr($geomArr, $geom, $blade, $conn);
            echo count($geomArr);
            $key = array_search($geom, $geomArr);
                //echo $geom;
                unset($geomArr[$key]);
            splitRecur($geomArr, $blade, $conn, $edgetb); 
        }
    }
    return $geomArr;

}

function createEdge($conn, $roadtb, $edgetb){
/*pg_query($conn,"begin");
$sqlstr = " drop table if exists ".$vertextb.";
create table ".$vertextb." (vid serial, roadid integer, geom geometry);
alter table ".$vertextb." add primary key(vid);";
$result = pg_query($conn,$sqlstr);
pg_query($conn,"end");*/

pg_query($conn,"begin");
$sqlstr = " drop table if exists ".$edgetb.";
create table ".$edgetb." (eid serial, roadid integer, geom geometry, startvt integer, endvt integer, cost float8);
alter table ".$edgetb." add primary key(eid);";
$result = pg_query($conn,$sqlstr);
pg_query($conn,"end");

$sqlstr = "select gid,st_astext(geom) from ".$roadtb."  order by gid";
$result = pg_query($conn,$sqlstr);
if(!$result)
{
    echo pg_last_error();
    pg_close($conn);
    exit(0);
}
while($row = pg_fetch_row($result)){
    
    pg_query($conn,"begin");
    $sqlstr_blades = " select gid,st_astext(geom) from ".$roadtb." where gid <> ".$row[0]." and (st_crosses(geom, st_geomfromtext('".$row[1]."',4326)) or st_touches(geom, st_geomfromtext('".$row[1]."',4326)))  and (st_within(st_startpoint(st_geomfromtext('".$row[1]."',4326)), geom) IS FALSE) and (st_within(st_endpoint(st_geomfromtext('".$row[1]."',4326)), geom) IS FALSE)";
    $result_blades = pg_query($conn,$sqlstr_blades);
    $i = 0;
    
    $tmpGeom = $row[1]; 
    if(!$result_blades)
    {
        echo pg_last_error();
        pg_close($conn);
        exit(0);        
    }
    echo pg_num_rows($result_blades)."\n";
    if(pg_num_rows($result_blades) == 0){
        echo "qq";
        $sqlstr_3 = "insert into $edgetb(geom,roadid) values(st_geomfromtext('$row[1]',4326), $row[0])";
        pg_query($conn,$sqlstr_3);
    }
    else
    {
        $tmpGeoms = array();
        while($row_blades = pg_fetch_row($result_blades)){
            $i = $i+1;
        
       /* $sqlstr_2 = " select st_astext((st_accum(st_geomfromtext('".$row_1[1]."',4326)))[1]) as geom ";
        $result_2 = pg_query($conn, $sqlstr_2);
        $Cutter = pg_fetch_result($result_2, 0, "geom");
        if($Cutter ==""){
            $Cutter = $row_1[1];
        }
        $i = $i+1;
        echo $i;*/
        if($i == 1){
            //addGeomArr($tmpGeoms, $row[1], $row_blades[1], $conn);
            array_push($tmpGeoms, $tmpGeom);
        }
        //else{
            //pg_query($conn,"begin");

        echo $row_blades[0]." :::: ".$row[0]."\n";
            $tmpGeoms = splitRecur($tmpGeoms, $row_blades[1], $conn, $edgetb);

            //pg_query($conn, "end");
            //$sqlstr_split = "select st_astext(st_split(st_geomfromtext('".$tmpGeom."',4326), st_geomfromtext('".$row_blades[1]."',4326))) as geom";
        //}
            
            //echo $tmpGeom;

            //$result_split = pg_query($conn, $sqlstr_split);
                //pg_query($conn,"end");

           /* if($result_split)
            {
                $tmpGeom = pg_fetch_result($result_split, 0, "geom");
                array_push($tmpGeoms, $tmpGeom);
            
            
            }
            else{
                echo count($row_blades);
            }*/
          
        }

        foreach ($tmpGeoms as $geom) {
            $sqlstr_3 = "insert into $edgetb(geom,roadid) values(st_geomfromtext('$geom',4326), $row[0])";
            pg_query($conn,$sqlstr_3);
        }
            /*$sqlstr_3 = "insert into ".$vertextb."(geom,roadid) select vertex,".$row[0]." from 
(select st_startpoint((st_dump(st_geomfromtext('".$tmpGeom."',4326))).geom) as vertex) foo;
insert into ".$vertextb."(geom) select st_endpoint(st_geomfromtext('".$row[1]."',4326))";
            pg_query($conn,$sqlstr_3);*/
    
    }
    pg_query($conn,"end");
   /* pg_query($conn,"begin");
    
    $sqlstr_3 = "insert into ".$vertextb."(geom) select st_endpoint(st_geomfromtext('$row[1]',4326))";
    pg_query($conn,$sqlstr_3);
    pg_query($conn,"end");*/
    //pg_query($conn,"begin");
    
    //pg_query($conn,"end");
}



//$sqlstr = "select eid,_geom,startvt,endvt,cost from ".$edgetb." where cost<4";
//$result = pg_query($conn,$sqlstr);
/*while($row = pg_fetch_row($result)){
    if($row[4]<4){
        $sqlstr_1 = "delete from ".$vertextb." where vid = (select vid from ".$vertextb."  where vid = ".$row[3].")";
        $result_1 = pg_query($conn,$sqlstr_1);
        if($result_1){
            $sqlstr_1 = "delete from ".$edgetb." where eid = ".$row[0];
            pg_query($conn,$sqlstr_1);
        }
    }
    
}*/

echo "done";

}

function createVertex($conn, $edgetb, $vertextb){
    pg_query($conn,"begin");
    $sqlstr = " drop table if exists ".$vertextb.";
    create table ".$vertextb." (vid serial, roadid integer, geom geometry);
    alter table ".$vertextb." add primary key(vid);";
    $result = pg_query($conn,$sqlstr);
    pg_query($conn,"end");

    /*$sqlstr = "select eid,st_astext(geom) from ".$edgetb."  order by eid";
    $result = pg_query($conn,$sqlstr);
    if(!$result)
    {
        echo pg_last_error();
        pg_close($conn);
        exit(0);
    }
    /*$sqlstr_3 = "insert into ".$vertextb."(geom,roadid) select vertex,".$row[0]." from 
(select st_startpoint((st_dump(st_geomfromtext('".$tmpGeom."',4326))).geom) as vertex) foo;
insert into ".$vertextb."(geom) select st_endpoint(st_geomfromtext('".$row[1]."',4326))";*/
    pg_query($conn,"begin");
    echo "aaa";
    $sqlstr = "insert into $vertextb(geom,roadid) (select st_startpoint(geom),roadid from $edgetb); 
    insert into $vertextb(geom,roadid) (select st_endpoint(geom),roadid from $edgetb);";
    $result = pg_query($conn,$sqlstr);
    echo $result ;
    pg_query($conn,"end");

    pg_query($conn,"begin");
    /*$sqlstr = "delete from $vertextb where geom in (select geom from $vertextb group by geom having count(geom) > 1) 
    and vid not in (select min(vid) from $vertextb group by geom having count(geom) > 1);";*/
    $sqlstr = "delete from $vertextb a where a.geom <> (select min(b.geom) from $vertextb b where a.geom = b.geom);";
    pg_query($conn,$sqlstr);
    pg_query($conn,"end");

    $sqlstr = "update ".$edgetb." e set startvt=vid, cost=st_length((st_transform(e.geom,9999))) from ".$vertextb." v where st_equals(v.geom, st_startpoint(e.geom));
update ".$edgetb." e set endvt=vid  from ".$vertextb." v where st_equals(v.geom, st_endpoint(e.geom))";
    pg_query($conn,$sqlstr);
    //pg_close($conn);
}

if($argc<5)
{
    echo 'Usage: php createTopology.php "connectstr" "roadtablename" "edgetablename" "vertextablename"';
    exit(0);
}
$connstr = pg_connect($argv[1]) or die(pg_last_error());
$roadtablename = $argv[2];
$edgetablename = $argv[3];
$vertextablename = $argv[4];
createEdge($connstr, $roadtablename, $edgetablename);
createVertex($connstr, $edgetablename, $vertextablename);
pg_close($connstr);
//delete from vertextb a where a.geom <> (select min(b.geom) from vertextb b where a.geom = b.geom);
//php createTopology.php "host=localhost port=5432 dbname=trajdb user=postgres password=2488990" "sample_beijingroad" "sample_edgetable" "sample_vertextable"
?>
