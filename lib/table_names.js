const { Pool } = require('pg')

exports.country_table_names = (pg_config) => {
  const { Pool } = require('pg')
  var dbPool = new Pool(pg_config)
  return new Promise((resolve, reject) => {
    console.log('About to query...');
    var st = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';";
    dbPool.query(st)
    .then(results => {
      results = results.rows;
      resolve(
        results.filter(  r => {
          return r.table_name.match(/[a-z]{3}_\d/);}
        )
        .map(r => { return r.table_name})
        .reduce((h, e) => {
          var country = e.match(/[a-z]{3}/)[0];
          if (h[country]) {
            h[country].push(e);
          } else {
            h[country] = [e];
          }
          //h[country] = h[country] ? h[country].push(e) : [e];
          return h;
        }, {})
      )
    });
  });
}
function customize_to_shapefile_source_specific_properties(admin_table, admin_level, source) {
  var st = 'SELECT gid, ST_Area(geom::geography)/1609.34^2 AS kilometers, ';
  switch(source) {
    case 'gadm2-8':
      for(var i = 0; i <= admin_level; i++) {
        st += '"' + admin_table + '"' + '.ID_' + i + ', ';
      }
      return st;
      break;
    // case 'santiblanko':
    //   return st += '"' + admin_table + '"' + '.dpto as ID_1,' + '"' + admin_table + '"' + '.wcolgen02_ as ID_2, '

    default:
      return st;
  }
}
exports.form_select_command = function(admin_table, shapefile_source, sum_or_mean) {
  var admin_level = parseInt(admin_table.match(/\d/)[0]);
  var aggregation = sum_or_mean === 'sum' ? 'SUM((ST_SummaryStats(ST_Clip(rast, geom))).sum)' : '(ST_SummaryStats(ST_Clip(rast, geom, -9999))).mean';

  switch(shapefile_source) {
    case 'gadm2-8':

      var st = customize_to_shapefile_source_specific_properties(admin_table, admin_level, shapefile_source);
      st += aggregation + ' FROM "' +
      admin_table +
      '" LEFT JOIN pop ON ST_Intersects("' + admin_table +

      '".geom, pop.rast) GROUP BY gid';

      if (sum_or_mean === 'sum' ) {
        st += ';';
      } else {
        st += ', mean;';
      }

      return st;
      break
    case 'santiblanko':
      // return "SELECT ST_Area(col_2_santiblanko.wkb_geometry::geography)/1609.34^2 AS kilometers, dpto, wcolgen02_ as id_2, SUM((ST_SummaryStats(ST_Clip(rast, wkb_geometry, -9999))).sum) FROM col_2_santiblanko LEFT JOIN pop ON ST_Intersects(col_2_santiblanko.wkb_geometry, pop.rast) GROUP BY id_1, id_2, kilometers;"
      return "SELECT ST_Area(col_2_santiblanko.wkb_geometry::geography)/1609.34^2 AS kilometers, dpto, wcolgen02_, (ST_SummaryStats(ST_Clip(rast, wkb_geometry, -9999))).mean FROM col_2_santiblanko LEFT JOIN pop ON ST_Intersects(col_2_santiblanko.wkb_geometry, pop.rast) GROUP BY dpto, wcolgen02_, kilometers, mean;"
    default:
      return st;
  }
}
