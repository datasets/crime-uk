var fs     = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;

var jsdom  = require('jsdom');
var request = require('request');
var csv = require('csv');


var linklist = 'http://police.uk/data';
var outlistfp = 'cache/linklist.json';
var zipsdir = 'cache/zips';

if (!path.existsSync('cache')) {
  fs.mkdirSync('cache');
}
if (!path.existsSync(zipsdir)) {
  fs.mkdirSync(zipsdir);
}

var out = [];

function scrapeLinkList() {
  jsdom.env({
    html: linklist,
    scripts: [
      'http://code.jquery.com/jquery.js'
    ],
    done: function(errors, window) {
      var $ = window.$;
      // street files
      $('#downloads .months table tr td:nth-child(2) a').each(function(idx, elem) {
        out.push( $(elem).attr('href') );
      });
      // street files
      $('#downloads .months table tr td:nth-child(3) a').each(function(idx, elem) {
        out.push( $(elem).attr('href') );
      });
      // not interested in outcomes atm (final column)

      // now save
      fs.writeFile(outlistfp, JSON.stringify(out, null, 2), function(err) {
        console.log('JSON saved to ' + outlistfp);
      });
    }
  });
}

function scrapeZip() {
  var links = JSON.parse(fs.readFileSync(outlistfp));
  links.slice(0, 10).forEach(function(link) {
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

// get weird error - think it is related to csv processing
// (node) warning: possible EventEmitter memory leak detected. 11 listeners added. Use emitter.setMaxListeners() to increase limit.
// emitter.setMaxListeners(50)

function consolidateZipToCsv() {
  var out = 'cache/streets.csv';
  var stream = fs.createWriteStream(out);
  
  // write the header
  stream.write('Month,Reported by,Falls within,Longitude,Latitude,Location,Crime type,Context\n');

  var links = JSON.parse(fs.readFileSync(outlistfp));
  links = links.slice(0, 9);

  function process(link, cb) {
    var fn = path.join(zipsdir, link.split('/').pop());
    var unzip = spawn('unzip', ['-p', fn])
    var stripfirstline = spawn('sed', ['1d']);
    unzip.stdout.on('data', function(data) {
      stripfirstline.stdin.write(data);
    });
    unzip.on('exit', function() {
      stripfirstline.stdin.end();
    });
    osgridToLonLatOnCsv(stripfirstline.stdout, stream, function() {
      console.log('Processed: ' + fn);
      cb()
    });
    // stripfirstline.stdout.on('data', function(data) {
    //  stream.write(data);
    // });
    // stripfirstline.on('exit', function() {
    //  console.log('Processed: ' + fn);
    //  cb()
    // });
  }
  var idx = 0;
  var looper = function() {
    if (idx >= links.length) {
      return;
    } else {
      process(links[idx], looper);
      idx += 1;
    }
  };
  looper();
}

function osgridToLonLatOnCsv(instream, outstream, cb) {
  var proj4js = require('proj4js');
  // hat-tip to Peter Hicks for providing the conversion spec
  // http://blog.poggs.com/2010/09/converting-osgb36-eastingsnorthings-to-wgs84-longitudelatitude-in-ruby/
  proj4js.defs["OSGB36"]="+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +datum=OSGB36 +units=m +no_defs";
  var srcproj = new proj4js.Proj('OSGB36');

  csv()
    .from.stream(instream)
    .to.stream(outstream)
    .transform(function(data, idx) {
      // data.unshift(data.pop());
      var eastingnorthing = data.slice(3,5);
      var eastingnorthing = [ parseInt(eastingnorthing[0]), parseInt(eastingnorthing[1]) ];
      if (eastingnorthing[0]) {
        var point = new proj4js.Point(eastingnorthing);
        var out = proj4js.transform(srcproj, proj4js.WGS84, point);
        data[3] = out.x;
        data[4] = out.y;
      }
      return data;
    })
    .on('end', function(count){
      console.log('Number of lines: '+count);
      cb();
    })
    .on('error', function(error){
      console.log(error.message);
    });
}

function demoConvert() {
  var infp = 'cache/streets-small.csv';
  var outfp = 'cache/streets-recoded.csv';
  var instream = fs.createReadStream(infp);
  var outstream = fs.createWriteStream(outfp);
  osgridToLonLatOnCsv(instream, outstream);
}

// scrapeZip();
consolidateZipToCsv();

