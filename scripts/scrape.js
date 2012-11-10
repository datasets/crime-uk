var fs     = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;

var jsdom  = require('jsdom');
var request = require('request');
var csv = require('csv');


var linklist = 'http://police.uk/data';
var outlistfp = 'cache/linklist.json';
var zipsdir = 'cache/zip';
var csvdir = 'cache/csv';

if (!path.existsSync('cache')) {
  fs.mkdirSync('cache');
}
if (!path.existsSync(zipsdir)) {
  fs.mkdirSync(zipsdir);
}
if (!path.existsSync(csvdir)) {
  fs.mkdirSync(csvdir);
}

function scrapeLinkList(cb) {
  var out = {
    'streets': [],
    'neighbourhoods': [],
    'outcomes': []
  };
  jsdom.env({
    html: linklist,
    scripts: [
      'http://code.jquery.com/jquery.js'
    ],
    done: function(errors, window) {
      var $ = window.$;
      // street files
      $('#downloads .months table tr td:nth-child(2) a').each(function(idx, elem) {
        out['streets'].push( $(elem).attr('href') );
      });
      // neighbourhoods files
      $('#downloads .months table tr td:nth-child(3) a').each(function(idx, elem) {
        out['neighbourhoods'].push( $(elem).attr('href') );
      });
      // not interested in outcomes atm (final column)

      // now save
      fs.writeFile(outlistfp, JSON.stringify(out, null, 2), function(err) {
        console.log('JSON saved to ' + outlistfp);
        cb();
      });
    }
  });
}

function scrapeZip() {
  var links = JSON.parse(fs.readFileSync(outlistfp))['streets'];
  links.forEach(function(link) {
    var fn = path.join(zipsdir, link.split('/').pop());
    var stream = fs.createWriteStream(fn);
    request(link)
      .pipe(stream)
      .on('close', function() {
        console.log(fn);
      });

    stream.on('error', function(e) {
      console.error(e);
    })
  });
}

function consolidateZipToCsv() {
  var stats = {
    total: 0,
    total_with_location: 0
  };
  // get weird error - think it is related to csv processing not unbinding listeners from this stream ...
  // (node) warning: possible EventEmitter memory leak detected. 11 listeners added. Use emitter.setMaxListeners() to increase limit.
  // emitter.setMaxListeners(50)
  // stream.setMaxListeners(0);
  
  // write the header
  // stream.write('Month,Reported by,Falls within,Longitude,Latitude,Location,Crime type,Context\n');
  // Drop "Falls within" as always repetition of Reported by AFAICt - see below for drop during processing
  var headers = 'Month,Reported by,Longitude,Latitude,Location,Crime type,Context'.split(',');

  var links = JSON.parse(fs.readFileSync(outlistfp))['streets'];
  links = links.slice(0,45);

  function process(link, cb) {
    var fn = link.split('/').pop();
    var csvpath = path.join(csvdir, fn + '.csv');
    var zipfp = path.join(zipsdir, link.split('/').pop());
    console.log('Processing: ' + zipfp + ' to ' + csvpath);
    var unzip = spawn('unzip', ['-p', zipfp])
    csv()
      .from.stream(unzip.stdout)
      .to.path(csvpath)
      .transform(function(data, idx) {
        if (idx == 0) {
          return headers;
        }
        stats.total += 1;
        // we will be a bit brutal - discard everything w/o a location
        // data[3] = Easting
        if (!data[3]) {
          return;
        }
        stats.total_with_location += 1;

        // fix up easting / northing to lon/lat
        var newval = convertEastingNorthingToLonLat(data.slice(3,5)); 
        data.splice(3,2,newval[0], newval[1]);

        // fix month
        data[0] = data[0] + '-01';

        // let's drop 'Falls within' but note first if "Reported by" and "Falls within" are different
        if (data[1] != data[2]) { // so unusual worth noting!!
          console.log(data); 
        }
        data.splice(1,1);
        return data;
      })
      .on('record', function(data) {
      })
      .on('end', function() {
        console.log(stats);
        cb();
      })
      .on('error', function(error) {
        console.log(error.message);
      });
  }
  var idx = 0;
  var looper = function() {
    if (idx >= links.length) {
      console.log(stats);
      return;
    } else {
      process(links[idx], looper);
      idx += 1;
    }
  };
  looper();
}

// Set up Projections for OSGB36 => WGS84 conversion
var proj4js = require('proj4js');
// hat-tip to Peter Hicks for providing the conversion spec
// http://blog.poggs.com/2010/09/converting-osgb36-eastingsnorthings-to-wgs84-longitudelatitude-in-ruby/
proj4js.defs["OSGB36"]="+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +datum=OSGB36 +units=m +no_defs";
var srcproj = new proj4js.Proj('OSGB36');

// eastingsnorthing must be an array (easting, northing)
// :return: [lon, lat]
var convertEastingNorthingToLonLat = function(eastingnorthing) {
  var eastingnorthing = [ parseInt(eastingnorthing[0]), parseInt(eastingnorthing[1]) ];
  if (eastingnorthing[0]) {
    var point = new proj4js.Point(eastingnorthing);
    var out = proj4js.transform(srcproj, proj4js.WGS84, point);
    // 5 decimal places is ~1m accuracy
    return [round(out.x,6), round(out.y,6)];
  } else {
    return null;
  }
};

function round(num, decPlaces) {
  var scale = Math.pow(10,decPlaces);
  return Math.round(num*scale)/scale;
}

function computeStats(filterString) {
  var links = JSON.parse(fs.readFileSync(outlistfp))['streets'];
  links = links.filter(function(link) {
    if (filterString) return (link.indexOf(filterString) != -1);
    else return true;
  });

  var stats = {
  };
  // Month,Reported by,Longitude,Latitude,Location,Crime type,Context
  var distinctRows = [0,1,5];
  var processRow = function(row) {
    var key = distinctRows.map(function(idx) {
      return row[idx];
    }).join(':::');
    if (key in stats) {
      stats[key] = stats[key] + 1;
    } else {
      stats[key] = 1;
    }
  }
  var writeStats = function(theStats, stream) {
    for (key in theStats) {
      var row = ['Month'];
      key.split(':::').forEach(function(val) {
        row.push(val);
      });
      row.push(theStats[key]);
      stream.write(row);
    };
  }

  headers = 'Period,Date,Body,Type,Count'.split(',');
  var outcsv = csv().to.path('cache/stats.csv');
  outcsv.write(headers);

  var idx = 0;
  function process(link, cb) {
    var csvpath = _csvFilePathFromLink(link);
    csv()
      .from.path(csvpath)
      .on('record', function(data, idx) {
        if (idx > 0) {
          processRow(data)
        }
      })
      .on('end', function() {
        console.log('Processed ' + csvpath);
        // now the loop
        if (idx < links.length-1) {
          idx += 1;
          writeStats(stats, outcsv);
          stats = {};
          cb(links[idx], cb)
        } else {
          // really finished - write stats
          writeStats(stats, outcsv);
        }
      });
  }
  process(links[0], process);
}

var _csvFilePathFromLink = function(link) {
  var fn = link.split('/').pop();
  var csvpath = path.join(csvdir, fn + '.csv');
  return csvpath;
}

// scrapeLinkList();
// scrapeZip();
// consolidateZipToCsv();

// removes 'node' and this script
args = process.argv.splice(2);

if (args.length == 0) {
  console.log('Commands are: fixtures | rebuild_db | load ');
  return;
}
if (args.length >= 2) {
  filter = args[1];
}

if (args[0] == 'consolidate') {
  consolidateZipToCsv();
} else if (args[0] == 'stats') {
  computeStats();
}

