var jsdom  = require('jsdom');
var fs     = require('fs');
var path = require('path');
// var jquery = fs.readFileSync("./jquery-1.7.1.min.js").toString();

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

var request = require('request');
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

var spawn = require('child_process').spawn;
function consolidateZipToCsv() {
  var out = 'cache/streets.csv';
  var stream = fs.createWriteStream(out);
  
  // write the header
  stream.write('Month,Reported by,Falls within,Easting,Northing,Location,Crime type,Context');

  var links = JSON.parse(fs.readFileSync(outlistfp));
  links = links.slice(0, 3);

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
    stripfirstline.stdout.on('data', function(data) {
      stream.write(data);
    });
    stripfirstline.on('exit', function() {
      console.log('Processed: ' + fn);
      cb()
    });
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

// scrapeZip();
consolidateZipToCsv();

