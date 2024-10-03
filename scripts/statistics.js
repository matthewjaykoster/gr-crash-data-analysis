import csv from 'csv-parser';
import { createReadStream, stat } from 'fs';

const CRASH_LOCATION_HEADER = 'CrashLocation';
const DATA_FILE_WORKSPACE_RELATIVE_PATH = '.\\data\\GR_Traffic_Crashes.csv';
const DAY_OF_WEEK_CONVERSION = [ 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday' ];
const MONTH_CONVERSION = [ 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December' ];

/**
 * Utility function to sort two strings alphabetically using the JS *.sort() function.
 * 
 * @param {string} a String A
 * @param {string} b String B
 * @returns {number} 1 if a > b, -1 if a < b, 0 otherwise.
 */
function _alphaSort( a, b ) {
  if ( a < b ) {
    return -1;
  }
  if ( a > b ) {
    return 1;
  }
  return 0;
}

/**
 * Converts a value and total into a percentage, returned as a string.
 * 
 * @param {number} value The value to compare against the total.
 * @param {number} total The total.
 * @returns {string} The percentage, formatted as a string.
 */
function _convertToPercentString( value, total ) {
  return `${( 100.0 * value / total ).toLocaleString( 'en-US', { maximumFractionDigits: 2 } )}%`;
}

/**
 * Adds 1 to a summation stored in the property of an object.
 * If the property does not exist, create it.
 * 
 * @param {*} obj The object which stores the summation.
 * @param {string} propertyName The name of the property in which the summation is stored.
 * @returns {void}
 */
function _incrementSummation( obj, propertyName ) {
  if ( !obj[ propertyName ] ) obj[ propertyName ] = 1;
  else obj[ propertyName ]++;
}


/**
 * Checks whether or not a value is a number stored as a string.
 * 
 * @param {*} str The value to check
 * @returns {boolean} true, if the value is a string storing a number. false otherwise.
 */
function _isNumeric( str ) {
  if ( typeof str != "string" ) return false; // we only process strings!  
  return !isNaN( str ) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
    !isNaN( parseFloat( str ) ); // ...and ensure strings of whitespace fail
}

/**
 * Logs a complete breakdown of a set of related stats to the console.
 * 
 * @param {string} title Title of breakdown.
 * @param {*} breakdown The object containing the numerical stats to include in the breakdown.
 * @param {number} totalCrashes The total number of crashes across the entire analysis.
 */
function _logConsoleBreakdown( title, breakdown, totalCrashes, sortFunction ) {
  console.log( title );
  let orderedProps = Object.keys( breakdown );
  if ( sortFunction ) {
    orderedProps = orderedProps.sort( sortFunction );
  }

  for ( const property of orderedProps ) {
    _logConsoleStat( `\t${property}`, breakdown[ property ], totalCrashes );
  }
}

/**
 * Writes a dividing line to the console.
 */
function _logConsoleLineDivider() {
  console.log( '================================================================' );
}

/**
 * Logs a statistic to the console, inclusive of title, value, and percentage.
 * 
 * @param {string} title The title of the stat.
 * @param {number} value The numerical value of the stat.
 * @param {number} totalCrashes The total number of crashes across the entire analysis.
 */
function _logConsoleStat( title, value, totalCrashes ) {
  console.log( `${title} - ${value} (${_convertToPercentString( value, totalCrashes )})` );

}

/**
 * Reject a promise resolution and log an error.
 * 
 * @param {Function} reject A rejection promise resolution function.
 * @param {Error} err A JS Error
 * @param {string} contextMessage Additional context for the error
 * @returns {void}
 */
function _rejectPromiseError( reject, err, contextMessage ) {
  console.error( err );
  reject( err );
}

/**
 * Compute a set of statistics about the crash data.
 * 
 * @param {Array<*>} data An array containing crash data parsed from the CSV.
 * @returns {*} An object containing a series of stats relevant to traffic crashes. 
 */
function computeStats( data ) {
  const stats = {
    totalCrashes: data.length,
    breakdowns: {
      dayOfWeek: {},
      hour: {},
      intersection: {},
      month: {},
      type: {},
      severity: {}
    }
  };

  for ( const item of data ) {
    const crashDate = new Date( item.CrashDateandTime );
    if ( !stats.earliestCrashDate || crashDate < stats.earliestCrashDate ) stats.earliestCrashDate = crashDate;
    if ( !stats.latestCrashDate || crashDate > stats.latestCrashDate ) stats.latestCrashDate = crashDate;

    // Breakdowns: Crash Type, Crash Severity, Month, Day of Week, Hour of Day
    _incrementSummation( stats.breakdowns.type, item.CrashType );
    _incrementSummation( stats.breakdowns.severity, item.CrashSeverity );
    _incrementSummation( stats.breakdowns.month, MONTH_CONVERSION[ new Date( item.CrashDateandTime ).getMonth() ] );
    _incrementSummation( stats.breakdowns.dayOfWeek, DAY_OF_WEEK_CONVERSION[ item.Dayoftheweek - 1 ] );
    _incrementSummation( stats.breakdowns.hour, ( item.HourofDay + 4 ) % 24 ); // Hour of day starts at 4am = 0 and wraps around until 3am = 23.
    _incrementSummation( stats.breakdowns.intersection, item.CrashLocation );

    // Included additional stats
    if ( item.PropertyDamageIndicator === true ) {
      _incrementSummation( stats, 'includesPropertyDamage' );
    }

    if ( item.AlcoholInvolved === true ) {
      _incrementSummation( stats, 'includesAlcohol' );
    }

    if ( item.AggressiveDriverInvolved === true ) {
      _incrementSummation( stats, 'includesAggressiveDriver' );
    }

    if ( item.BicycleInvolved === true ) {
      _incrementSummation( stats, 'includesBicycle' );
    }

    if ( item.CellPhoneInvolved === true ) {
      _incrementSummation( stats, 'includesCellPhone' );
    }

    if ( item.AnimalInvolved === true ) {
      _incrementSummation( stats, 'includesAnimal' );
    }

    if ( item.DrugInvolved === true ) {
      _incrementSummation( stats, 'includesDrugs' );
    }
  }

  // Calc intersection top 10
  const intersectionTop10 =
    Object
      .entries( stats.breakdowns.intersection )
      .sort( ( a, b ) => b[ 1 ] - a[ 1 ] )
      .slice( 0, 10 );
  stats.breakdowns.intersectionTop10 = Object.fromEntries( intersectionTop10 );

  return stats;
}

/**
 * Write stastical data to the console.
 * 
 * @param {*} stats An object containing a computed set of stats about GR Crash Data.
 * @returns {void}
 */
function displayStats( stats ) {
  _logConsoleLineDivider();
  const earliestCrashDate = stats.earliestCrashDate.toLocaleDateString( 'en-US', { year: 'numeric', month: 'short', day: 'numeric' } );
  const latestCrashDate = stats.latestCrashDate.toLocaleDateString( 'en-US', { year: 'numeric', month: 'short', day: 'numeric' } );

  console.log( 'Disclaimer:' );
  console.log( `This analysis provides crash information for Grand Rapids between ${earliestCrashDate} and ${latestCrashDate}.` );
  console.log( 'Data is sourced from the City of Grand Rapids, provided at https://www.grandrapidsmi.gov/GRData/Police-Data. Data is not updated live, but stored in a local file which is manually downloaded.' );
  _logConsoleLineDivider();

  console.log( `Total Crashes: ${stats.totalCrashes}` );
  _logConsoleLineDivider();

  _logConsoleBreakdown( 'Most Dangerous Intersections:', stats.breakdowns.intersectionTop10, stats.totalCrashes, _alphaSort );
  _logConsoleLineDivider();

  _logConsoleBreakdown( 'By Type:', stats.breakdowns.type, stats.totalCrashes, _alphaSort );
  _logConsoleLineDivider();

  _logConsoleBreakdown( 'By Severity:', stats.breakdowns.severity, stats.totalCrashes, _alphaSort );
  _logConsoleLineDivider();

  _logConsoleBreakdown( 'By Month:', stats.breakdowns.month, stats.totalCrashes, ( a, b ) => MONTH_CONVERSION.indexOf( a ) - MONTH_CONVERSION.indexOf( b ) );
  _logConsoleLineDivider();

  _logConsoleBreakdown( 'By Day of Week:', stats.breakdowns.dayOfWeek, stats.totalCrashes, ( a, b ) => DAY_OF_WEEK_CONVERSION.indexOf( a ) - DAY_OF_WEEK_CONVERSION.indexOf( b ) );
  _logConsoleLineDivider();

  _logConsoleBreakdown( 'By Hour:', stats.breakdowns.hour, stats.totalCrashes );
  _logConsoleLineDivider();

  console.log( 'By Misc:' );
  _logConsoleStat( '\tAggressive Driving', stats.includesAggressiveDriver, stats.totalCrashes );
  _logConsoleStat( '\tInvolved Alcohol ', stats.includesAlcohol, stats.totalCrashes );
  _logConsoleStat( '\tInvolved Cell Phone ', stats.includesCellPhone, stats.totalCrashes );
  _logConsoleStat( '\tInvolved Drugs ', stats.includesDrugs, stats.totalCrashes );
  _logConsoleStat( '\tProperty Damage', stats.includesPropertyDamage, stats.totalCrashes );
  _logConsoleStat( '\tWith Animal', stats.includesAnimal, stats.totalCrashes );
  _logConsoleStat( '\tWith Cyclist', stats.includesBicycle, stats.totalCrashes );
}

/**
 * Load crash data from the local file and convert it into a JS array for easy consumption.
 * 
 * @param {string} fileName The name of the file which contains the crash data.
 * @returns {Array<*>} An array containing one js object per row of CSV data.
 */
async function loadCrashData( fileName ) {
  return new Promise( ( resolve, reject ) => {
    const results = [];
    createReadStream( fileName )
      .pipe( csv( {
        mapHeaders: ( { header } ) => header.replace( /\s/g, '' ),
        mapValues: ( { header, index, value } ) => {
          // Parse:
          //  - Crash Location cleanup (basically ensuring that A & B and B & A are counted the same)
          //  - yes/no values into booleans
          //  - numerics into numbers
          if ( header === CRASH_LOCATION_HEADER ) return value.replace( /\s\s+/g, ' ' ).trim().split( ' & ' ).sort().join( ' & ' );
          else if ( value.toLowerCase() === 'yes' ) return true;
          else if ( value.toLowerCase() === 'no' ) return false;
          else if ( _isNumeric( value ) ) return +value;

          return value;
        }
      } ) )
      .on( 'error', err => _rejectPromiseError( reject, err, 'Error thrown parsing CSV.' ) )
      .on( 'data', data => results.push( data ) )
      .on( 'error', err => _rejectPromiseError( reject, err, 'Error thrown during data results aggregation.' ) )
      .on( 'end', () => resolve( results ) )
      .on( 'error', err => _rejectPromiseError( reject, err, 'Error thrown during stream end resolution.' ) );
  } );
}

/**
 * Load crash data from the local CSV file and generate some stats about the information.
 */
async function main() {
  const data = await loadCrashData( DATA_FILE_WORKSPACE_RELATIVE_PATH );
  const stats = computeStats( data );
  displayStats( stats );
}

await main();