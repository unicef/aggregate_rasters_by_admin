const aggregate = require('./aggregate_raster_by_country_shapefiles');
const fs = require('fs');
const config = require('./config');
const bluebird = require('bluebird');
const raster_dir = config.save_raster_dir;
const countries = fs.readdirSync(raster_dir)
.reduce((h, country) => {
  h[country] = fs.readdirSync(config.save_raster_dir + country)
  .find(file => {
    return file.match(/tif$/);
  });
  return h;
}, {})

var country_codes = Object.keys(countries);
bluebird.each(country_codes, country_code => {
  console.log('Getting', country_code)
  return aggregate.aggregate_raster_by_all_country_shapefiles(
    'population',
    country_code,
    raster_dir + country_code + '/' + countries[country_code],
    'worldpop',
    'sum',
    'gadm2-8'
  )
}, {concurrency: 1})
.then(process.exit)
