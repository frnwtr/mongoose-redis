var
	util = require('util'),
	events = require('events'),
	redis = require('redis');

function RedisEvent(redisClient, prefix, channelsList) {
	events.EventEmitter.call(this);

	var self = this;
	this.prefix = prefix + ":";

	self._connectedCount = 0;

	if (!channelsList || channelsList.length == 0) {
		var channelsList = [];
	}

	this.channelsList = channelsList;

	this.pubRedis = redisClient.pub;
	this.pubRedis.on('error', function (e) {
		console.log(e);
	});
	this.pubRedis.on('ready', function () {
		self._connectedCount++;
		if (self._connectedCount == 2) {
			self.emit('ready');
		}
	});
	this.pubRedis.on('end', function () {
		self._connectedCount--;
	});

	this.subRedis = redisClient.sub;
	this.subRedis.on('error', function (e) {
		console.log(e);
	});
	this.subRedis.on('ready', function () {
		self._connectedCount++;
		if (channelsList && channelsList.length > 0) {
			self._subscribe();
		}
		if (self._connectedCount == 2) {
			self.emit('ready');
		}
	});
	this.subRedis.on('end', function () {
		self._connectedCount--;
	});

	this.subRedis.on("message", this._onMessage.bind(this));
}
util.inherits(RedisEvent, events.EventEmitter);

RedisEvent.prototype._subscribe = function () {
	var self = this;
	this.channelsList.forEach(function (channelName) {
		self.subRedis.subscribe(self.prefix + channelName);
	});
}
RedisEvent.prototype.addChannel = function (channelName) {
	var self = this;
	if (self.channelsList.indexOf(self.prefix + channelName) < 0) {
		self.channelsList.push(self.prefix + channelName);
		self.subRedis.subscribe(self.prefix + channelName);
	};
}
RedisEvent.prototype.listChannels = function () {
	var self = this;
	return self.channelsList;
}
RedisEvent.prototype._onMessage = function (channel, message) {
	var data = null,
		eventName = null;
	try {
		data = JSON.parse(message);
		if (data && data.event) {
			eventName = channel + ':' + data.event;
		}
	} catch (e) {}

	if (data && eventName) {
		this.emit(eventName, data.payload);
	}
}

RedisEvent.prototype.pub = function (eventName, payload) {
	var split = eventName.split(':');
	if (split.length != 3) {
		console.log("ev warning: eventName '%s' is incorrect", eventName);
		return false;
	}

	var data = {
		prefix: split[0],
		channel: split[1],
		event: split[2],
		payload: payload
	};

	this.pubRedis.publish(split[0] + ":" + split[1], JSON.stringify(data), function () {});
}

RedisEvent.prototype.quit = function () {
	this.subRedis.quit();
	this.pubRedis.quit();
}
module.exports = RedisEvent;
