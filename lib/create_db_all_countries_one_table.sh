
#!/bin/bash
# Used by psql_import_shapefiles
# Set up a database to show how to accumulate population density
# to an admin boundary with the following query:
# SELECT
# 	gid,
# 	name_2,
# 	SUM((ST_SummaryStats(ST_Clip(rast, geom))).sum)
# FROM admin
# LEFT JOIN pop ON ST_Intersects(admin.geom, pop.rast)
# GROUP BY gid;


# upper=`echo "$1" | tr /a-z/ /A-Z/`
# upper_to_file=$upper'_adm';

# Check if database exists...
if [ -z $(psql -lqt | cut -d \| -f 1 | grep $1) ]; then
	echo "Database doesn't exist, creating it now..."
	createdb $1
	psql $1 -c "CREATE EXTENSION postgis;"
fi

test_table_exists=`psql $1 -c "select * from pg_tables where schemaname='public'" | grep $1`

if [ "$test_table_exists" ]; then
  echo "Table exists"
  shp2pgsql -s 4326 -a $3 $1| psql $1
else
  echo "Table does NOT exist"
  shp2pgsql -s 4326 -D -I $3 $1 | psql $1
fi
#
# # # Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
# # raster2pgsql -Y -s 4326 -t 100x100 -I data/HTI_ppp_v2b_2015_UNadj.tif pop | psql pop_density
#
#
# test=`psql 'all_countries_one_table' -c "select * from pg_tables where schemaname='public'" | grep 'all_countries_one_tablle'`
#
# shp2pgsql -s 4326 -a $3 $1| psql $1
# shp2pgsql -s 4326 -D -I $3 $1 | psql $1


#
# # psql "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'AFG';"

# SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'ssd_2' AND pid <> pg_backend_pid();
