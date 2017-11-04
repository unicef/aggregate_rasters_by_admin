// node aggregate_raster_by_all_countries.js -k aegypti --tif aegypti -s simon_hay -m mean -f gadm2-8
// node aggregate_raster_by_all_countries.js --tif 2015.01.02.tif -s chirps -k precipitation -m mean
const async = require('async');
const bluebird = require('bluebird');
const fs = require('fs');
const ArgumentParser = require('argparse').ArgumentParser;
const exec = require('child_process').exec;
const config = require('./config');
const mkdirp = require('mkdirp');
const pg_config = config.pg_config;
const save_to_dir = config.save_to_dir;
const db_queries = require('./lib/db_queries');
const countries_db = config.pg_config.database;
const shp_source = 'gadm2-8';
const {Pool} = require('pg')
const dbPool = new Pool(pg_config)
const parser = new ArgumentParser({
  version: '0.0.1',
  addHelp: true,
  description: 'Aggregate a csv of airport by admin 1 and 2'
});

parser.addArgument(
  ['-t', '--tif'],
  {help: 'Name of tif to import'}
);
parser.addArgument(
  ['-s', '--source'],
  {help: 'Source of tif to import'}
);

parser.addArgument(
  ['-k', '--kind'],
  {help: 'population, egypti, or precipitation'}
)

parser.addArgument(
  ['-m', '--sum_or_mean'],
  {help: 'sum or mean'}
)

parser.addArgument(
  ['-f', '--shapefile'],
  {help: 'Shapefile source: gadm2-8'}
)

const args = parser.parseArgs();
const tif = args.tif;
const kind = args.kind;
const tif_source = args.source;
// var shapefile_source = args.shapefile;
const sum_or_mean = args.sum_or_mean;

// Only for precipitation ?
/**
 * Execute command
 * @param{String} country - country code
 * @return{Promise} Fulfilled directory is created
 */
function mkdir(country) {
  return new Promise((resolve, reject) => {
    // if (kind.match(/precipitation/)) {
      country, tif, kind, tif_source, sum_or_mean
      mkdirp(save_to_dir + kind + '/' +
      tif_source + '/' + shp_source + '/' +
      country, (err) => {
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
 * @param{String} country - 3 letter country ISO code taken from wikipedia
 * @return{Promise} Fulfilled when all countries processed
 */
function process_country(country) {
  return new Promise((resolve, reject) => {
    return mkdir(country)
    .then(() => {
      return scan_raster(country);
    })
    .then(() => {
      resolve();
    })
  })
}

/**
 * Aggregate raster by all countries
 * @return{Promise} Fulfilled when all countries processed
 */
aggregate_raster_by_all_countries = () => {
  console.log('Processing', tif)
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Drop table pop if exists
      function(callback) {
        // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
        let command = 'psql ' + countries_db +
        ' -c "DROP TABLE IF EXISTS pop"';
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

        let path = save_to_dir + kind + '/' + tif_source + '/';
        if (kind.match(/(aegypti|albopictus)/)) {
          path = config[kind].local
        }
        let command = 'raster2pgsql -Y -s 4326 -t 100x100 -I '
        + path + tif + '.tif pop | psql ' + countries_db;
        console.log(command);
        execute_command(command)
        .then(response => {
          callback();
        });
      },

      // Retrieve list of country names
      function(callback) {
        db_queries.get_country_names(pg_config)
        .then(country_names => {
          bluebird.each(country_names, (country, i) => {
            return process_country(country).then(() => {
            });
          }, {concurrency: 1})
          .then(callback);
        });
      },
      function(callback) {
        // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
        let command = 'psql ' + countries_db +
        ' -c "DROP TABLE IF EXISTS pop"'
        execute_command(command)
        .then(response => {
          console.log(response);
          callback();
        });
      }
    ], function() {
      console.log('done!');
      resolve();
    });
  })
}

// Start here
aggregate_raster_by_all_countries()
.then(() => {
  console.log('All complete!');
  process.exit();
})

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
    if (r.mean) {
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
    }
    return h
  }, {})
  return results
}

/**
 * save set
 * @param{number} admin_level - admin_level
 * @param{object} set - 3 letter country ISO code taken from wikipedia
 * @return{Promise} Fulfilled when country processed
 */
function save_set(admin_level, set) {
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
       path += country + '/'
    //  }

    Object.keys(set).forEach(admin_id => {
      set[admin_id] = Object.keys(set[admin_id]).reduce((h, k) => {
        if (set[admin_id][k]) {
          h[k] = set[admin_id][k]
        }
        return h;
      }, {})
    })

     fs.writeFile(path +
     country +
     '^' +
     admin_level +
     '^' + tif +
     '^' + tif_source +
     '^' + amount +
     '^' + kilo_sum +
     '.json',
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
 * @param{object} sets - 3 letter country ISO code taken from wikipedia
 * @return{Promise} Fulfilled when country processed
 */
function save_sets(sets) {
  return new Promise((resolve, reject) => {
    bluebird.each(Object.keys(sets), set => {
      return save_set(set, sets[set])
    }, {concurrency: 1})
    .then(resolve);
  })
}

/**
 * scan_raster
 * @param{String} country - 3 letter country ISO code taken from wikipedia
 * @return{Promise} Fulfilled when country processed
 */
function scan_raster(country) {
  console.log('About to query...***', country, shp_source, sum_or_mean);
  return new Promise((resolve, reject) => {
    let st = db_queries.form_select_command(
      country, shp_source, sum_or_mean
    );
    console.log(st)
    dbPool.query(st)
    .then(results => {
      results = results.rows;
      let sets = group_by_admin(results);
      save_sets(sets)
      .then(resolve)
    })
    .catch(error => {
      reject(error)
    })
  });
}
