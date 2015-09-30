var Worker = require('pigato').Worker;
var debug = require('hitd-debug')('hitd-handler');
var async = require('async');


module.exports = function(registerEndPoint, conf, pRules, finalCb) {

	var workers = {};

	var stopped = false;
	var stop = function(cb) {
		if (stopped) {
			return;
		}
		stopped = true;

		async.each(Object.keys(workers), function(key, cb) {
			workers[key].once('stop', function() {
				console.log("woerk stopped", key);
				cb();
			})
			workers[key].stop();
		}, function() {
			cb();
		})
	};

	async.forEachOf(pRules, function(rule, key, cb) {
		debug('registering handling rule %s', JSON.stringify(key));

		var worker = new Worker(registerEndPoint, key, conf);
		workers[key] = worker;
		var alreadyCalled = false;

		worker.on('start', function() {

			if (!alreadyCalled) {
				debug('worker for rule %s started and conf %s', key, JSON.stringify(
					conf));
				cb();
			}
			alreadyCalled = true;
		});
		worker.start();

		worker.on('stop', function() {
			debug('Worker with key stopeed %s %s %s', key, registerEndPoint, JSON.stringify(
				conf));

		})
		worker.on('request', function(inp, rep) {
			var called = 0;
			debug('Request on worker with key %s inp is %s', inp.key, JSON.stringify(
				inp));

			var toSend =
				rule(inp.key, {
					body: inp.body,
					clientId: inp.clientId
				}, function(err, code, content) {

					if (code < 100) {
						//sepcial internal command
						rep.write(code);
						rep.write(content);
						return;
					}
					called++;

					if (called > 1) {
						return;
					}
					if (err) {
						rep.write('500');
						rep.end(err);
						return;
					}

					debug('reply on %s code  %s', key, code);
					rep.write(code);
					rep.end(content);
				});
		});

	}, function() {
		finalCb(null, {
			stop: stop
		});
	});
};
