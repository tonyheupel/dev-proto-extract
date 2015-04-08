// A Node.js version of the crawl export tool
var fs = require('fs'),
	path = require('path'),
	util = require('util'),
	elasticsearch = require('elasticsearch'),
	async = require('async'),
	mkdirp = require('mkdirp'),
	cheerio = require('cheerio'),
	ArgumentParser = require('argparse').ArgumentParser,
	packageInfo = require('../package.json');


function doWork(host, index, pageSize, concurrency, articleBodySelector) {
	console.log("ARTICLE BODY SELECTOR: " + articleBodySelector);

	var getArticleContent = function (content) {
		$ = cheerio.load(content);
    	return $(articleBodySelector).html();
	}

	var getHeadContent = function(content) {
		$ = cheerio.load(content);
    	return $('head').html();
	}

	var buildHTMLDocument = function (head, content) {
		return util.format('<html>\n<head>\n\t\t%s\n\t</head>\n\t<body>\n%s\n</body>\n</html>', head, content);
	};
	var writeHitToFile = function (hit, cb) {
		var url = hit._source.url;

		// Get rid of http://, trailing /,  and convert query string to use -
		if (url.lastIndexOf('/') === (url.length - 1)) {
			url = url.substring(0, url.length - 1)
		}

		var filename = url.replace(/https?\:\/\//, '').replace(/[\?=&]/, '-') + '.html';
		var outputPrefix = path.join('./output', index);
		var pathToFile = path.join(outputPrefix, filename.substring(0, filename.lastIndexOf('/')));

		var outputFilename = path.join(outputPrefix, filename);

		console.log('processing: "%s" in "%s"', filename, pathToFile);
		
		mkdirp(pathToFile, function (err) {
			if (err) {
				console.error('problem writing file "%s": %s', filename, err)
				setImmediate(function () { cb(err) });
				return;
			}

			var fileContents = hit._source.rawContent;
			var head = getHeadContent(fileContents);
			var content = getArticleContent(fileContents, articleBodySelector);

			if (content === null || content === '') {
				console.log('no content found...falling back to full HTML: "%s"', filename);
				content = fileContents;
			} else {
				content = buildHTMLDocument(head, content);
			}

			var fileStream = fs.createWriteStream(outputFilename, { flags: 'w'});
			fileStream.on('error', function(err) {
				console.error('problem with file "%s": %s:', outputFilename, err);
				setImmediate(function () { cb(err) });
			});

			fileStream.write(content);
			fileStream.end();
			setImmediate(function () { cb(null) });
		})	
	};

	var enqueueHits = function (hits) {
		workQueue.push(hits); // queue them in a batch
	};


	var client = elasticsearch.Client({
	  host: host
	});

	var getMoreUntilDone = function (err, resp) {
		if (err) {
			client.close();
			console.error('Error: ' + err);
			return;
		}

		if (!resp) {
			hasMoreDocuments = false;
			client.close();
			return;
		}

		var hits = resp.hits.hits;
		enqueueHits(hits);

		if (hits.length > 0) {
			// now we can call scroll over and over
			client.scroll({
				scrollId: resp._scroll_id,
	      		scroll: '30s'
	    	}, getMoreUntilDone);
		} else {
			client.close();
		}
	}

	var createWorkQueue = function (concurrency) {
		return async.queue(function (task, cb) {
			writeHitToFile(task, function(err) {
				setImmediate(function() { cb(err); });  // No more process.nextTick to keep it async	
			});
		}, concurrency);
	};
	// Initialize work queue
	var workQueue = createWorkQueue(concurrency);
	workQueue.drain = function () {
		console.log('Queue drained.');

		if (!hasMoreDocuments) {
			// Don't quit until we got all the documents
			process.exit(0);	
		}
	};


	// Do work until done
	var hasMoreDocuments = true; // Set to false when no more results came back

	client.search({
	  index: 	index,
	  size: 	pageSize,
	  scroll: 	'30s',
	  _source: 	['url', 'title', 'description', 'rawContent']
	}, getMoreUntilDone);

}


var parser = new ArgumentParser({
  version: packageInfo.version,
  addHelp: true,
  description: packageInfo.description
});
parser.addArgument(
  [ '-host', '--host' ],
  {
    help: 'the hostname and port for Elasticsearch',
    defaultValue: 'crawled.content.infospace.com:9200'
  }
);
parser.addArgument(
  [ '-i', '--index' ],
  {
    help: 'the Elasticsearch index to page through',
    required: true,
  }
);
parser.addArgument(
	['-p', '--pageSize'],
	{
		help: 'the number of results to return for each page of requests as it iterates through the results pages',
		defaultValue: 20,
		type: 'int'
	}
);
parser.addArgument(
	['-c', '--concurrency'],
	{
		help: 'the number of files to process concurrently',
		defaultValue: 5,
		type: 'int'
	}
);
parser.addArgument(
	['-a', '--articleBodySelector'],
	{
		help: 'the DOM selector to extract the article body content',
		defaultValue: 'body'
	}
);

var args = parser.parseArgs();
doWork(args['host'], args['index'], args['pageSize'], args['concurrency'], args['articleBodySelector']);
