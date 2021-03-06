var _ = require('underscore');
var async = require('async');
var mongoose = require('mongoose');
var kue = require('kue');
var redisEvent = require('./redis-event');
var util = require('util');
var events = require('events');
var redis = require('redis');
var Schema = require('mongoose').Schema;

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

function newJob(jobs, prefix, action, chan, doc, cb) {
	var channel = action + chan;
	var evCont = doc.toObject();

	var job = jobs.create(channel, doc);
	job.on('complete', function () {
		evCont.status = "completed";
		evCont.progress = 100;
		evCont.job = job.id;
		ev.pub(prefix + ":queue:" + channel, evCont);
	}).on('failed', function () {
		evCont.status = "failed";
		evCont.job = job.id;
		ev.pub(prefix + ":queue:" + channel, evCont);
	}).on('progress', function (progress) {
		evCont.status = "progress";
		evCont.job = job.id;
		evCont.progress = progress;
		ev.pub(prefix + ":queue:" + channel, evCont);
	}).on('enqueue', function () {
		evCont.status = "enqueue";
		evCont.job = job.id;
		evCont.progress = 0;
		ev.pub(prefix + ":queue:" + channel, evCont);
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
				ev.pub(prefix + ":stats:queue", obj);
			}
		});
		cb(err, job);
	});
}

function mongooseRedis(schema, options) {
	events.EventEmitter.call(this);
	var redisClientKue = null;
	var prefix = "q";
	var softDelete = false;
	if (options.redisClient) {
		var redisClientPub = options.redisClient.pub;
		var redisClientSub = options.redisClient.sub;
		redisClientKue = options.redisClient.kue;
		prefix = options.redisClient.kue.prefix;
	} else {
		var redisClientPub = redis.createClient('localhost', 6379);
		var redisClientSub = redis.createClient('localhost', 6379);
	}
	if (options.softDelete) {
		var softDelete = options.softDelete;
	}

	ev = new redisEvent({
		pub: redisClientPub,
		sub: redisClientSub
	}, prefix, ['create', 'update', 'remove', 'queue', 'stats', 'delete', 'restore']);
	var model = null;
	schema.queue('hook', ['construct', function () {}]);
	schema.queue('construct', []);

	schema.queue = function (name, args) {
		this.callQueue.splice(-1, 0, [name, args])
		return this;
	};
	var channel = null;
	var self = this;
	var newRecords = [],
		delettingRecords = [],
		restoringRecords = [];
	var queue = {
		create: false,
		remove: false,
		update: false,
		delete: false,
		restore: false
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
			ev.on(prefix + ":create:" + channel, function (data) {
				model.emit("created", data);
			});

			ev.on(prefix + ":update:" + channel, function (data) {
				model.emit("updated", data);
			});

			ev.on(prefix + ":remove:" + channel, function (data) {
				model.emit("removed", data);
			});

			ev.on(prefix + ":queue:create" + channel, function (data) {
				model.emit("queue:create", data);
			});

			ev.on(prefix + ":queue:remove" + channel, function (data) {
				model.emit("queue:remove", data);
			});

			ev.on(prefix + ":queue:update" + channel, function (data) {
				model.emit("queue:update", data);
			});

			if (softDelete) {
				ev.on(prefix + ":delete:" + channel, function (data) {
					model.emit("deleted", data);
				});

				ev.on(prefix + ":restore:" + channel, function (data) {
					model.emit("restored", data);
				});

				ev.on(prefix + ":queue:delete" + channel, function (data) {
					model.emit("queue:delete", data);
				});

				ev.on(prefix + ":queue:restore" + channel, function (data) {
					model.emit("queue:restore", data);
				});
			}

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

	if (softDelete) {
		schema.add({
			deleted: Boolean
		});

		if (softDelete.deletedAt === true) {
			schema.add({
				deletedAt: {
					type: Date
				}
			});
		}

		if (softDelete.deletedAt === true) {
			schema.add({
				deletedBy: Schema.Types.ObjectId
			});
		}

		schema.pre('save', function (next) {
			if (!this.deleted) {
				this.deleted = false;
			}
			next();
		});

		schema.methods.delete = function (first, second) {
			var callback = typeof first === 'function' ? first : second,
				deletedBy = second !== undefined ? first : null;

			if (typeof callback !== 'function') {
				throw ('Wrong arguments!');
			}

			this.deleted = true;

			if (schema.path('deletedAt')) {
				this.deletedAt = new Date();
			}

			if (schema.path('deletedBy')) {
				this.deletedBy = deletedBy;
			}
			delettingRecords.push(this._id);
			this.save(callback);
		};

		schema.methods.restore = function (callback) {
			this.deleted = false;
			this.deletedAt = undefined;
			this.deletedBy = undefined;
			restoringRecords.push(this._id);
			this.save(callback);
		};
	}

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
			ev.pub(prefix + ":create:" + channel, doc);
			if (queue.create) {
				newJob(model.jobs, prefix, "create", channel, doc, function (err, job) {});
			}
		} else {
			var indexDelete = delettingRecords.indexOf(doc._id);
			if (indexDelete > -1) {
				delettingRecords.splice(indexDelete, 1);
				ev.pub(prefix + ":delete:" + channel, doc);
				if (queue.create) {
					newJob(model.jobs, prefix, "delete", channel, doc, function (err, job) {});
				}
			} else {
				var indexRestore = restoringRecords.indexOf(doc._id);
				if (indexRestore > -1) {
					restoringRecords.splice(indexRestore, 1);
					ev.pub(prefix + ":restore:" + channel, doc);
					if (queue.create) {
						newJob(model.jobs, prefix, "restore", channel, doc, function (err, job) {});
					}
				} else {
					ev.pub(prefix + ":update:" + channel, doc);
					if (queue.update) {
						newJob(model.jobs, prefix, "update", channel, doc, function (err, job) {});
					}
				}
			}
		}
	});
	schema.post('remove', function (doc) {
		ev.pub(prefix + ":remove:" + channel, doc);
		if (queue.remove) {
			newJob(model.jobs, prefix, "remove", channel, doc, function (err, job) {});
		}
	});

};

util.inherits(mongooseRedis, events.EventEmitter);
module.exports = exports = mongooseRedis;
