
/**
 * get_country_names
 * @param{Object} pg_config
 * @return{Promise} Fulfilled when country names are retreived
 */
exports.get_country_names = (pg_config) => {
  const {Pool} = require('pg')
  const dbPool = new Pool(pg_config)
  return new Promise((resolve, reject) => {
    console.log('About to query...!!');
    const st = 'SELECT iso FROM ' +
    pg_config.table_all_admins +
    ' group by iso;';
    dbPool.query(st)
    .then(results => {
      results = results.rows;

      resolve(
        results.map(e => {
          return e.iso.replace(/\s+$/, '')
        })
      )
    });
  });
}

/**
 * customize_to_shapefile_source_specific_properties
 * @param{String} source
 * @param{String} admin_table
 * @return{Promise} Fulfilled when country names are retreived
 */
function customize_to_shapefile_source_specific_properties(
  source, admin_table
) {
  let st = 'SELECT gid, iso, ' +
  'ST_Area(geom::geography)/1609.34^2 AS kilometers, ';
  switch (source) {
    case 'gadm2-8':
      for (
        let i = 0;
        i <= 5;
        i++
      ) {
        st += '"' + admin_table + '"' + '.ID_' + i + ', ';
      }
      return st;
      break;
    // case 'santiblanko':
    //   return st += '"' + admin_table + '"' + '.dpto as ID_1,' + '"' + admin_table + '"' + '.wcolgen02_ as ID_2, '

    default:
      return st;
  }
  return st
}

/**
 * form_select_command
 * @param{String} country
 * @param{String} shapefile_source
 * @param{String} sum_or_mean
 * @param{Integer} admin_level
 * @return{Promise} Fulfilled when country names are retreived
 */
exports.form_select_command = (
  country, shapefile_source, sum_or_mean, admin_level
) => {
  let admin_table = 'all_admins';
  let aggregation = sum_or_mean === 'sum' ?
  'SUM((ST_SummaryStats(ST_Clip(rast, geom))).sum)' :
  '(ST_SummaryStats(ST_Clip(rast, geom, -9999))).mean';
  switch (shapefile_source) {
    case 'gadm2-8':
      let st = customize_to_shapefile_source_specific_properties(
        shapefile_source,
        admin_table
      );
      st += aggregation + ' FROM "' +
      admin_table +
      '" LEFT JOIN raster_file ON ST_Intersects("' + admin_table +

      '".geom, raster_file.rast) ' +
      'where id_0 is ' + test_admin_level(admin_level, 0) + ' null ' +
      'and id_1 is ' + test_admin_level(admin_level, 1) + ' null ' +
      'and id_2 is ' + test_admin_level(admin_level, 2) + ' null ' +
      'and id_3 is ' + test_admin_level(admin_level, 3) + ' null ' +
      'and id_4 is ' + test_admin_level(admin_level, 4) + ' null ' +
      'and id_5 is ' + test_admin_level(admin_level, 5) + ' null ' +
      'and iso = \'' + country.toUpperCase() + '\' GROUP BY gid';

      if (sum_or_mean === 'sum' ) {
        st += ';';
      } else {
        st += ', mean;';
      }
      return st;
      break
    case 'santiblanko':
      // return "SELECT ST_Area(col_2_santiblanko.wkb_geometry::geography)/1609.34^2 AS kilometers, dpto, wcolgen02_ as id_2, SUM((ST_SummaryStats(ST_Clip(rast, wkb_geometry, -9999))).sum) FROM col_2_santiblanko LEFT JOIN pop ON ST_Intersects(col_2_santiblanko.wkb_geometry, pop.rast) GROUP BY id_1, id_2, kilometers;"
      return 'SELECT ' +
      'ST_Area(col_2_santiblanko.wkb_geometry::geography)/1609.34^2 ' +
      'AS kilometers, dpto, wcolgen02_, ' +
      '(ST_SummaryStats(ST_Clip(rast, wkb_geometry, -9999))).mean ' +
      'FROM col_2_santiblanko LEFT JOIN raster_file ON ' +
      'ST_Intersects(col_2_santiblanko.wkb_geometry, raster_file.rast) ' +
      'GROUP BY dpto, wcolgen02_, kilometers, mean;'
    default:
      return st;
  }
}

/**
 * test_admin_level
 * @param{String} admin_level
 * @param{Integer} admin_level_and
 * @return{string} not or ''
 */
function test_admin_level(admin_level, admin_level_and) {
  // if (admin_level === 0) {
  //   return 'not'
  // } else {
    if (admin_level_and <= admin_level) {
      return 'not';
    } else {
      return '';
    }
  // }
}
