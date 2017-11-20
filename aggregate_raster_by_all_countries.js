// node aggregate_raster_by_all_countries.js -r 100x100
const aggregate = require('./aggregate_raster_by_country_shapefiles');
const ArgumentParser = require('argparse').ArgumentParser;
const fs = require('fs');
const config = require('./config');
const bluebird = require('bluebird');
const raster_dir = config.save_raster_dir;
const parser = new ArgumentParser({
  version: '0.0.1',
  addHelp: true,
  description: 'Aggregate a csv of airport by admin 1 and 2'
});

parser.addArgument(
  ['-r', '--resolution'],
  {help: 'widthxheight resolution of raster tiles'}
);
parser.addArgument(
  ['-c', '--country'],
  {help: 'Country to begin with if process exited part way through'}
);
const args = parser.parseArgs();
const tile_dimensions = args.resolution;
const start_country = args.country;
let go_live = start_country ? false : true

const countries = fs.readdirSync(raster_dir)
.reduce((h, country) => {
  h[country] = fs.readdirSync(config.save_raster_dir + country)
  .find(file => {
    return file.match(/tif$/);
  });
  return h;
}, {})

const country_codes = Object.keys(countries);
bluebird.each(country_codes, country_code => {
  console.log('Getting', country_code)
  if (start_country && start_country.match(/country_code/i)) {
    go_live = true
  }
  if (!go_live) {
    return
  }
  return aggregate.aggregate_raster_by_all_country_shapefiles(
    country_code,
    'population',
    raster_dir + country_code + '/' + countries[country_code],
    'worldpop',
    'gadm2-8',
    'sum',
    tile_dimensions
  )
}, {concurrency: 1})
.then(process.exit)
