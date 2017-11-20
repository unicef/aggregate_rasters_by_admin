// node aggregate_raster_by_all_countries.js -k aegypti --tif aegypti -s simon_hay -m mean -f gadm2-8
// node aggregate_raster_by_all_countries.js -k population --tif ~/downloads/aedes_maps_public/aegypti.tif -s worldpop -m sum -f gadm2-8
// node aggregate_raster_by_all_countries.js -k population --tif ../data/rasters/population/cub/CUB_ppp_v2b_2015_UNadj.tif -s worldpop -m sum -f gadm2-8
// node aggregate_raster_by_all_countries.js --tif 2015.01.02.tif -s chirps -k precipitation -m mean
const async = require('async');
const bluebird = require('bluebird');
const fs = require('fs');
const exec = require('child_process').exec;
const config = require('./config');
const mkdirp = require('mkdirp');
const pg_config = config.pg_config;
const save_to_dir = config.save_to_dir;
const db_queries = require('./lib/db_queries');
const countries_db = config.pg_config.database;

const {Pool} = require('pg')
const dbPool = new Pool(pg_config)


// Only for precipitation ?
/**
 * Execute command
 * @param{String} country - country code
 * @param{String} kind - kind
 * @param{String} tif_source - tif_source
 * @param{String} shp_source - shp_source
 * @param{String} tile_dimensions - new_dir_name
 * @return{Promise} Fulfilled directory is created
 */
function mkdir(country, kind, tif_source, shp_source, tile_dimensions) {
  return new Promise((resolve, reject) => {
    // if (kind.match(/precipitation/)) {
      // country, tif, kind, tif_source, sum_or_mean
      let path_to_dir = save_to_dir + kind + '/' +
      tif_source + '/' + shp_source + '/' + tile_dimensions;
      console.log('About to mkdir', path_to_dir);
      mkdirp.sync(path_to_dir);
      mkdirp(path_to_dir + '/' + country, (err) => {
          if (err) console.error(err)
          else resolve();
      });
    // } else {
    //   resolve();
    // }
  })
}

/**
 * Execute command
 * @param{String} command - sql command
 * @return{Promise} Fulfilled when command finishes execution
 */
function execute_command(command) {
  return new Promise((resolve, reject) => {
    exec(command, (err, stdout, stderr) => {
      if (err) {
        console.error(err);
      }
      resolve(stdout);
    });
  });
}

/**
 * process_country
 * @param  {string} country admin
 * @param  {string} kind admin
 * @param  {string} tif admin
 * @param  {string} tif_source admin
 * @param  {string} shp_source admin
 * @param  {string} sum_or_mean admin
 * @param  {string} tile_dimensions admin
 * @return{Promise} Fulfilled when all countries processed
 */
function process_country(country,
  kind,
  tif,
  tif_source,
  shp_source,
  sum_or_mean,
  tile_dimensions
) {
  return new Promise((resolve, reject) => {
    mkdir(country.toUpperCase(), kind, tif_source, shp_source, tile_dimensions)
    .then(() => {
      bluebird.each([5, 4, 3, 2, 1, 0], (admin_level, index) => {
        return scan_raster(
          country,
          kind,
          admin_level,
          tif,
          tif_source,
          shp_source,
          sum_or_mean,
          tile_dimensions
        );
      }, {concurrency: 1})
      .then(resolve)
    })
  })
}

/**
 * Aggregate raster by all countries
 * @param  {string} country admin
 * @param  {string} kind admin
 * @param  {string} tif admin
 * @param  {string} tif_source admin
 * @param  {string} shp_source admin
 * @param  {string} sum_or_mean admin
 * @param  {string} tile_dimensions admin
 * @return{Promise} Fulfilled when all countries processed
 */
exports.aggregate_raster_by_all_country_shapefiles = (
  country,
  kind,
  tif,
  tif_source,
  shp_source,
  sum_or_mean,
  tile_dimensions) => {
  console.log('Processing', tif)
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Drop table pop if exists
      function(callback) {
        // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
        let command = 'psql ' + countries_db +
        ' -c "DROP TABLE IF EXISTS raster_file"';
        console.log(command);

        execute_command(command)
        .then(response => {
          console.log(response);
          callback();
        });
      },

      // Import raster to database
      function(callback) {
        console.log('About to add', tif)
        // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index

        // let path = save_to_dir + kind + '/' + tif_source + '/';
        // if (kind.match(/(aegypti|albopictus)/)) {
        //   path = config[kind].local
        // }

        let command = 'raster2pgsql -Y -s 4326 -I -t ' +
        tile_dimensions + ' ' +
        tif + ' raster_file | psql ' + countries_db;
        console.log(command);
        execute_command(command)
        .then(response => {
          callback();
        });
      },

      // Retrieve list of country names
      function(callback) {
        process_country(
          country,
          kind,
          tif,
          tif_source,
          shp_source,
          sum_or_mean,
          tile_dimensions
        )
        .then(callback)
      },
      // function(callback) {
      //   // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
      //   let command = 'psql ' + countries_db +
      //   ' -c "DROP TABLE IF EXISTS raster_file"'
      //   console.log(command)
      //   execute_command(command)
      //   .then(response => {
      //     console.log(response);
      //     callback();
      //   });
      // }
    ], function() {
      console.log('done!');
      resolve();
    });
  })
}

// exports.aggregate_raster_by_country_shapefiles(kind, country, tif, source, sum_or_mean, shapefile) {
//   return new Promise((resolve, reject) => {
//
//   })
// }
//
// // Start here
// aggregate_raster_by_all_countries()
// .then(() => {
//   console.log('All complete!');
//   process.exit();
// })

/**
 * Returns admin id per coordinates
 * @param  {Object} shape_obj admin
 * @return {Object} admin
 */
function add_admin_id(shape_obj) {
  let iso = shape_obj.iso.toLowerCase();
  let ids = Object.keys(shape_obj).filter(k => {
    return k.match(/^ID_\d+/i) && shape_obj[k];
  }).map(k => {
    if (shape_obj[k]) {
      return shape_obj[k].replace(/\s+/, '')
    }
  }).join('_')

  let admin_id = [iso, ids, 'gadm2-8'].join('_');
  shape_obj.admin_id = admin_id;
  return shape_obj;
}

/**
 * Returns admin id per coordinates
 * @param  {Object} obj admin
 * @return {Object} admin
 */
function remove_pesky_quote(obj) {
  Object.keys(obj).forEach(key => {
    if (obj[key] && typeof(obj[key]) === 'string') {
      obj[key] = obj[key].replace(/('|\s+)/g, '');
    }
  });
  return obj
}
/**
 * group_by_admin
 * @param  {Object} results admin
 * @return {Object} admin
 */
function group_by_admin(results) {
  results.forEach(r => {
    r = remove_pesky_quote(r)
    r = add_admin_id(r)
  });
  // Remove objects with no value
  results = results.reduce((h, r) => {
    // if (r.mean) {
      let ids = r.admin_id.match(/_[0-9_]+_/)[0]
      .replace(/^_/, '')
      .replace(/_$/, '')
      .split(/_/)
      let len = ids.length -1
      if (h[len]) {
        h[len][r.admin_id] = r;
      } else {
        h[len] = {};
        h[len][r.admin_id] = r;
      }
    // }
    return h
  }, {})
  return results
}

/**
 * save set
 * @param  {string} admin_level admin
 * @param  {string} set admin
 * @param  {string} kind admin
 * @param  {string} tif admin
 * @param  {string} tif_source admin
 * @param  {string} shp_source admin
 * @param  {string} sum_or_mean admin
 * @param  {string} tile_dimensions admin
 * @return{Promise} Fulfilled when country processed
 */
function save_set(admin_level, set, kind, tif,
  tif_source, shp_source, sum_or_mean, tile_dimensions) {
  return new Promise((resolve, reject) => {
    // // var pop_sum = parseInt(results.reduce((s, r) => { return s + r.sum }, 0));
    let admin_ids = Object.keys(set);
    let kilo_sum = parseInt(admin_ids.reduce((fl, r) => {
      return fl + set[r].kilometers
    }, 0));

    let sums_or_means = admin_ids.map(e => {
      return set[e][sum_or_mean];
    });

    let sum = 0;
    let amount = null;
    sums_or_means.forEach(e => {
      if (e) {
        sum += e;
       }
    });

    if (sum_or_mean === 'mean') {
      let avg = 0;
      if (sum) {
        avg = sum/admin_ids.length;
      }
      amount = Math.ceil(avg * 100000) / 100000;
    } else {
      amount = sum;
    }

     let country = set[admin_ids[0]].iso
    //  // content = content + results.map(r => {return [file, r.sum || 0, r.dpto, r.wcolgen02_, 'col_0_' + r.dpto + '_' + r.wcolgen02_ + '_santiblanko'].join(" ") }).join("\n")
     let path = save_to_dir + kind + '/'
     + tif_source + '/' + shp_source + '/';
    //  if (kind.match(/precipitation/)) {
       path += tile_dimensions + '/' + country + '/'
    //  }

    Object.keys(set).forEach(admin_id => {
      set[admin_id] = Object.keys(set[admin_id]).reduce((h, k) => {
        if (set[admin_id][k]) {
          h[k] = set[admin_id][k]
        }
        return h;
      }, {})
    })

    let path_file = path +
    country +
    '^' +
    admin_level +
    '^' +
    tif.match(/([^\\/]+)(\.tif$)/)[1] +
    '^' + tile_dimensions +
    '^' + tif_source +
    '^' + amount +
    '^' + kilo_sum +
    '.json';
    console.log(path_file);
     fs.writeFile(path_file,
     JSON.stringify(set), (err) => {
       if (err) {
         console.log(err);
         console.log('Please manually create this dir.');
         process.exit();
       }
       console.log('done!', country, admin_level)
       resolve();
      })
  })
}

/**
 * save sets
 * @param  {Object} sets
 * @param  {string} kind
 * @param  {string} tif
 * @param  {string} tif_source
 * @param  {string} shp_source
 * @param  {string} sum_or_mean
 * @param  {string} tile_dimensions
 * @return{Promise} Fulfilled when country processed
 */
function save_sets(sets,
  kind, tif, tif_source, shp_source, sum_or_mean, tile_dimensions) {
  return new Promise((resolve, reject) => {
    bluebird.each(Object.keys(sets), set => {
      return save_set(set, sets[set],
        kind, tif, tif_source, shp_source,
        sum_or_mean, tile_dimensions)
    }, {concurrency: 1})
    .then(resolve);
  })
}

/**
 * scan_raster
 * @param  {string} country admin
 * @param  {string} kind admin
 * @param  {string} admin_level
 * @param  {string} tif admin
 * @param  {string} tif_source admin
 * @param  {string} shp_source admin
 * @param  {string} sum_or_mean admin
 * @param  {string} tile_dimensions admin
 * @return{Promise} Fulfilled when country processed
 */
function scan_raster(
  country,
  kind,
  admin_level,
  tif,
  tif_source,
  shp_source,
  sum_or_mean,
  tile_dimensions ) {
  let start_time = Date.now();
  console.log(start_time,
    'About to query...***', country, admin_level, shp_source, sum_or_mean
  );
  return new Promise((resolve, reject) => {
    console.log(shp_source, '!!!!')
    let st = db_queries.form_select_command(
      country, shp_source, sum_or_mean, admin_level
    );
    console.log(st)

    dbPool.query(st)
    .then(results => {
      console.log('DONE!',
      ((Date.now() - start_time)/1000),
      country, admin_level);
      results = results.rows;
      let sets = group_by_admin(results);
      save_sets(sets,
        kind,
        tif,
        tif_source,
        shp_source, sum_or_mean, tile_dimensions)
      .then(resolve)
    })
    .catch(error => {
      reject(error)
    })
  });
}
