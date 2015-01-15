var _ = require('underscore');
var async = require('async');
var mongoose = require('mongoose');
var kue = require('kue');
var redisEvent = require('./redis-event');
var util = require('util');
var events = require('events');
var redis = require('redis');

var ev = {};


function get(obj) {
	var pending = 0,
		res = {},
		callback, done;

	return function _(arg) {
		switch (typeof arg) {
		case 'function':
			callback = arg;
			break;
		case 'string':
			++pending;
			obj[arg](function (err, val) {
				if (done) return;
				if (err) return done = true, callback(err);
				res[arg] = val;
				--pending || callback(null, res);
			});
			break;
		}
		return _;
	};
}

function newJob(jobs,prefix, action, chan, doc, cb) {
	var channel = action + chan;
	var evCont = doc.toObject();

	var job = jobs.create(channel, doc);
	job.on('complete', function () {
		evCont.status = "completed";
		evCont.progress = 100;
		evCont.job = job.id;
		ev.pub(prefix+"queue:" + channel, evCont);
	}).on('failed', function () {
		evCont.status = "failed";
		evCont.job = job.id;
		ev.pub(prefix+"queue:" + channel, evCont);
	}).on('progress', function (progress) {
		evCont.status = "progress";
		evCont.job = job.id;
		evCont.progress = progress;
		ev.pub(prefix+"queue:" + channel, evCont);
	}).on('enqueue', function () {
		evCont.status = "enqueue";
		evCont.job = job.id;
		evCont.progress = 0;
		ev.pub(prefix+"queue:" + channel, evCont);
	});
	job.removeOnComplete(true).save(function (err) {
		get(jobs)
			('inactiveCount')
			('completeCount')
			('activeCount')
			('failedCount')
			('delayedCount')
			('workTime')
			(function (err, obj) {
				if (err) {
					console.log({
						error: err.message
					});
				} else {
					ev.pub(prefix+"stats:queue", obj);
				}
			});
		cb(err, job);
	});
}

function mongooseRedis(schema, options) {
	events.EventEmitter.call(this);
	var redisClientKue=null;
	var prefix="q:";
	if (options.redisClient) {
		var redisClientPub = options.redisClient.pub;
		var redisClientSub = options.redisClient.sub;
		redisClientKue = options.redisClient.kue;
		prefix=options.redisClient.kue.prefix+":";
	} else {
		var redisClientPub = redis.createClient('localhost', 6379);
		var redisClientSub = redis.createClient('localhost', 6379);
	}

	ev = new redisEvent({
		pub: redisClientPub,
		sub: redisClientSub
	}, ['create', 'update', 'remove', 'queue', "stats"]);
	var model = null;
	schema.queue('hook', ['construct', function () {}]);
	schema.queue('construct', []);

	schema.queue = function (name, args) {
		this.callQueue.splice(-1, 0, [name, args])
		return this;
	};
	var channel = null;
	var self = this;
	var newRecords = [];
	var queue = {
		create: false,
		remove: false,
		update: false
	};

	if (options.queue) {
		_.each(queue, function (e, i) {
			if (options.queue[i]) {
				queue[i] = true;
			}
		});
	}

	schema.pre('construct', function (next) {
		model = this.model(this.constructor.modelName);
		if (!channel) {
			channel = this.constructor.modelName;
			ev.on(prefix+"create:" + channel, function (data) {
				model.emit("created", data);
			});

			ev.on(prefix+"update:" + channel, function (data) {
				model.emit("updated", data);
			});

			ev.on(prefix+"remove:" + channel, function (data) {
				model.emit("removed", data);
			});

			ev.on(prefix+"queue:create" + channel, function (data) {
				model.emit("queue:create", data);
			});

			ev.on(prefix+"queue:remove" + channel, function (data) {
				model.emit("queue:remove", data);
			});

			ev.on(prefix+"queue:update" + channel, function (data) {
				model.emit("queue:update", data);
			});

			/*	ev.on("stats:queue" + channel, function (data) {
					model.emit("stats:queue", data);
				});*/
			model.jobs = kue.createQueue(redisClientKue);
			
			model.queueStats = function (cb) {
				get(model.jobs)
					('inactiveCount')
					('completeCount')
					('activeCount')
					('failedCount')
					('delayedCount')
					('workTime')
					(function (err, obj) {
						if (err) {
							cb(err, null);
						} else {
							cb(null, obj);
						}
					});
			};
			model.queueTypes = function (cb) {
				model.jobs.types(function (err, types) {
					cb(err, types);
				});
			}
			model.queueProcess = function (what, execute) {
				model.jobs.process(what + channel, function (job, done) {
					execute(job, done);
				});
			};
			model.emit("init", channel);
		}

		next();
	});

	schema.pre('save', function (next) {
		if (this.isNew) {
			newRecords.push(this._id);
		}
		next();
	});
	schema.post('init', function (doc) {});
	schema.post('validate', function (doc) {});
	schema.post('save', function (doc) {
		var indexNew = newRecords.indexOf(doc._id);
		if (indexNew > -1) {
			newRecords.splice(indexNew, 1);
			ev.pub(prefix+"create:" + channel, doc);
			if (queue.create) {
				newJob(model.jobs, "create", channel, doc, function (err, job) {});
			}
		} else {
			ev.pub(prefix+"update:" + channel, doc);
			if (queue.update) {
				newJob(model.jobs, "update", channel, doc, function (err, job) {});
			}
		}
	});
	schema.post('remove', function (doc) {
		ev.pub(prefix+"remove:" + channel, doc);
		if (queue.remove) {
			newJob(model.jobs, "remove", channel, doc, function (err, job) {});
		}
	});

};

util.inherits(mongooseRedis, events.EventEmitter);
module.exports = exports = mongooseRedis;
