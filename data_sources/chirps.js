// node scripts/chirps -s ../../rasters/precipitation/chirps -d 2017-01-11
var async = require('async');
var ArgumentParser = require('argparse').ArgumentParser;
var bluebird = require('bluebird');
var moment = require('moment');
var config = require('../config');
var ftp = require('ftp-get');
var fs = require('fs');
var exec = require('child_process').exec;
var save_to_dir = config.save_to_dir + 'precipitation/chirps/'
// var aggregate_raster = require('../aggregate_raster_by_all_countries');

var parser = new ArgumentParser({
  version: '0.0.1',
  addHelp: true,
  description: 'Aggregate a csv of airport by admin 1 and 2'
});

parser.addArgument(
  ['-s', '--store'],
  {help: 'Directory to save zipped raster'}
);

parser.addArgument(
  ['-d', '--date'],
  {help: 'date to process: YYYY-MM-DD'}
);

var args = parser.parseArgs();
var raster_store = args.store + '/';
var date = moment(args.date);

function download(obj) {
  return new Promise((resolve, reject) => {
    async.waterfall([
      function(callback) {
        // Check if zipped raster has been downloaded
        // If not, download it and save to rasters save directory.
        console.log('Does', obj.dir + obj.file_name, 'exist?')
        fs.exists(obj.dir + obj.file_name, function(exists) {
          if (!exists) {
            console.log('gzip does not exist');
            ftp.get(obj.url,  obj.dir + obj.file_name, function (err, res) {
              if (err) {
                console.log(err)
              }
              callback();
            })
          } else {
            console.log('gzip exists');
            callback();
          }
        });
      },

      function(callback) {

        // Unzip raster to another directory to be imported to postgres
        var command = 'gunzip -c ' + raster_store + obj.file_name + ' > ' + save_to_dir + obj.file_name.replace(/.gz/, '') ;
        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback();
        });
      },

      // function(callback) {
      //   aggregate_raster.aggregate_raster_by_all_countries(obj.day, 'chirps', 'precipitation', 'mean')
      //   .then(() => {
      //     callback();
      //   })
      // },
      function(callback) {

        var command = 'node aggregate_raster_by_all_countries.js --tif ' + obj.day + ' -s chirps -k precipitation -m mean';
        console.log(command);

        exec(command, {maxBuffer: 8192 * 5000}, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback();
        });
      },

      function(callback) {

        var command = 'rm ' + save_to_dir + obj.file_name.replace(/.gz/, '');
        console.log(command);

        exec(command, (err, stdout, stderr) => {
          if (err) {
            console.error(err);
          }
          callback();
        });
      },
    ], function (err, result) {
      setTimeout(() => {return resolve();}, 300)
        // result now equals 'done'
    });

  })
}

var dates = [];
var c = 0;
var file_type = '.tif.gz';
var year = date.year();

var total_days = moment(String(year) + '-12-31').diff(date, 'days')
while(c < total_days) {
  var day = date.add(c, 'days').format('YYYY.MM.DD');
  var url = 'ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/' + year + '/chirps-v2.0.' + day + file_type;

  dates.push(
    {
      url: url,
      type: file_type,
      // dir: save_to_dir,
      dir: raster_store,
      day: day,
      year: year,
      file_name: day + file_type
    }
  )
  c++;
}

bluebird.each(dates, (obj, index) => {

  return download(obj);
}, {concurrency: 1})
.then(() => {
  console.log('done with all');
  process.exit();
})
