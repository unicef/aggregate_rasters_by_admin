// node aggregate_raster_by_all_countries.js -k aegypti --tif aegypti -s simon_hay -m mean -f gadm2-8
// node aggregate_raster_by_all_countries.js --tif 2015.01.02.tif -s chirps -k precipitation -m mean
var async = require('async');
var bluebird = require('bluebird');
var pg = require('pg');
var fs = require('fs');
var ArgumentParser = require('argparse').ArgumentParser;
var exec = require('child_process').exec;
var command = 'psql all_countries -c "\\dt" ';
var config = require('./config');
var mkdirp = require('mkdirp');
var pg_config = config.pg_config;
var save_to_dir = config.save_to_dir;
var table_names = require('./lib/table_names');

var parser = new ArgumentParser({
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

var args = parser.parseArgs();
var tif = args.tif;
var kind = args.kind;
var tif_source = args.source;
// var shapefile_source = args.shapefile;
var sum_or_mean = args.sum_or_mean;

function mkdir(table, kind, tif_source) {
  [country, admin_level, shp_source] = table.split(/_/);
  return new Promise((resolve, reject) => {
    if (kind.match(/precipitation/)) {
      mkdirp(save_to_dir + kind + '/' + tif_source + '/' + shp_source + '/' + country, function (err) {
          if (err) console.error(err)
          else resolve();
      });
    } else {
      resolve();
    }
  })
}

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

function process_tables(country, country_tables, tif, kind, tif_source, sum_or_mean) {
  return new Promise((resolve, reject) => {
    bluebird.each(country_tables, table => {
      // Create direcotry for country if doesn't exist.
      return mkdir(table, kind, tif_source)
      .then(() => {
        return scan_raster(country, table, tif, kind, tif_source, sum_or_mean);
      });
    }, {concurrency: 1})
    .then(() => {
      resolve();
    })
  })
}

exports.aggregate_raster_by_all_countries = (tif, tif_source, kind, sum_or_mean) => {
  console.log(tif, tif_source, kind, sum_or_mean)
  console.log('Processing', tif)
  return new Promise((resolve, reject) => {
    async.waterfall([
      // Drop table pop if exists
      function(callback) {
        // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
        var command = 'psql all_countries_one_db -c "DROP TABLE IF EXISTS pop"';
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

        var path = save_to_dir + kind + '/' + tif_source + '/';
        if (kind.match(/(aegypti|albopictus)/)) {
          path = config[kind].local
        }
        var command = "raster2pgsql -Y -s 4326 -t 100x100 -I " + path + tif + ".tif pop | psql all_countries_one_db";
        console.log(command);
        execute_command(command)
        .then(response => {
          callback();
        });
      },

      // Retrieve list of tables by country, admin_level, shapefile source
      function(callback) {
        table_names.country_table_names(pg_config)
        .then(tables => {
          bluebird.each(Object.keys(tables), (country, i) => {
            console.log(country, '*****', tables[country])
            return process_tables(country, tables[country], tif, kind, tif_source, sum_or_mean).then(() => {
            });
          }, {concurrency: 1})
          .then(callback);
        });
      },

      function(callback) {
        // Use EPSG:4326 SRS, tile into 100x100 squares, and create an index
        var command = 'psql all_countries_one_db -c "DROP TABLE IF EXISTS pop"'
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

this.aggregate_raster_by_all_countries(tif, tif_source, kind, sum_or_mean)
.then(() => {
  console.log('All complete!');
  process.exit();
})

function scan_raster(country, admin_table, tif,  kind, tif_source, sum_or_mean) {
  var results = [];
  console.log('About to query...');
  var shp_source = admin_table.split('_')[admin_table.split('_').length-1];
  return new Promise((resolve, reject) => {
    pg.connect(pg_config, (err, client, done) => {
      var st = table_names.form_select_command(admin_table, shp_source, sum_or_mean);
      console.log(st);
      var query = client.query(st);

      // Stream results back one row at a time
      query.on('row', (row) => {
        results.push(row);
      });
      // After all data is returned, close connection and return results
      query.on('end', () => {
       // var pop_sum = parseInt(results.reduce((s, r) => { return s + r.sum }, 0));
        var kilo_sum = parseInt(results.reduce((s, r) => { return s + r.kilometers}, 0));
        var results2 = results.map(e => { return e[sum_or_mean];});

        var sum = 0;
        var amount = null;
        results2.forEach(e => { if (e) { sum += e;}});

        if (sum_or_mean === 'mean') {
          var avg = 0;
          if (sum) {
            avg = sum/results2.length;
          }
          amount =  Math.ceil(avg * 100000) / 100000;
        } else {
          amount = sum;
        }

        // content = content + results.map(r => {return [file, r.sum || 0, r.dpto, r.wcolgen02_, 'col_0_' + r.dpto + '_' + r.wcolgen02_ + '_santiblanko'].join(" ") }).join("\n")
        var path = save_to_dir + kind + '/' + tif_source + '/' + shp_source + '/';
        if (kind.match(/precipitation/)) {
          path += country + '/'
        }
        fs.writeFile(path  +
        admin_table +
        '^' + tif +
        '^' + tif_source +
        '^' + amount +
        '^' + kilo_sum +
        '.json',
        JSON.stringify(results), (err) => {
          if (err) {
            console.log(err);
            console.log('Please manually create this dir.');
            process.exit();
          }
          console.log('done!', country, admin_table)
          done();
          resolve();
        });
      });
    });
  })
}
