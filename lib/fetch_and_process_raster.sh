upper=`echo "$1" | tr /a-z/ /A-Z/`'-POP'
if [ $1 == "bra" ]; then
  upper="Americas-POP-1KM"
fi
if [[ "$1" =~ ^(irn|jor|tur|tkm)$ ]]; then
  upper="Asia-POP-1KM"
fi

if [ $1 == "nga" ]; then
  upper="Africa-POP-1KM"
fi

# Drop the table if it exists

psql all_countries_one_db -c "DROP TABLE IF EXISTS pop;"

if [ -d $4$1 ]; then
  for file in $4$1\/*tif; do
    tif=${file##*/}
  done
fi

echo "http://www.worldpop.org.uk/data/hdx/?dataset=$upper"
echo '9999'
echo $tif
if [ ! $tif ]; then

	wget "http://www.worldpop.org.uk/data/hdx/?dataset=$upper" -O $2$3/$1.zip
	unzip -d $2$3/$1 $2$3/$1.zip
  echo $2$3/$1 $2$3/$1.zip

  file=$2$3/$1/*.tif

  if [ -f $file ]; then
    # create ../../rasters/population/afg/
    if [ ! -d $4$1 ]; then
      mkdir $4$1
    fi
    echo "File exists! $file"
    fbname=$(basename $file)
    echo $fbname
    echo $file
    echo $2$3/$1*
    cp $file $4$1/$fbname
    echo $1
    rm -rf $2$3/$1
    rm -rf $2$3/$1/$1.zip
    tif=$fbname
  fi
fi

echo raster2pgsql -Y -s 4326 -t 25x25 -I $4$1\/$tif pop | psql all_countries_one_db

# Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
#raster2pgsql -Y -s 4326 -t 100x100 -I $4$1\/$tif pop | psql all_countries
raster2pgsql -Y -s 4326 -t 20x20 -I $4$1\/$tif pop | psql all_countries_one_db
# DO NOT REMOVE OR EDIT THIS LINE
echo $4$1\/$tif
