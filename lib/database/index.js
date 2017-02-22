/**
 * Module Dependencies
 */

var _ = require('lodash'),
    async = require('async'),
    Errors = require('waterline-errors'),
    WaterlineCriteria = require('waterline-criteria'),
    Utils = require('../utils'),
    Schema = require('./schema'),
    Aggregate = require('../aggregates'),
    hasOwnProperty = Utils.object.hasOwnProperty;

/**
 * An Interface for interacting with Redis.
 *
 * @param {Object} connection
 */

var Database = module.exports = function(connection) {

  // Hold the connection
  connection = connection || { connection: {} };

  this.connection = connection.connection;

  this.definedCollections = {};

  // Hold Schema for the database
  this.schema = new Schema(connection);

  return this;
};


///////////////////////////////////////////////////////////////////////////////////////////
/// PUBLIC METHODS
///////////////////////////////////////////////////////////////////////////////////////////


/**
 * Configure the "database" for a collection.
 *
 * @param {String} collectionName
 * @param {Object} config
 * @param {Object} schema
 * @param {Function} callback
 * @api public
 */

Database.prototype.configure = function configure(collectionName, schema) {
  var name = collectionName.toLowerCase();
  this.schema.registerCollection(name, schema);
};


/**
 * Sync the "database" to redis
 *
 * @param {Function} callback
 * @api public
 */

Database.prototype.sync = function sync(cb) {
  this.schema.sync(cb);
};

/**
 * Describe the schema for a collection.
 *
 * @param {String} collectionName
 * @param {Function} callback
 * @api public
 */

Database.prototype.describe = function describe(collectionName, cb) {
  var name = collectionName.toLowerCase(),
      desc = this.schema.retrieve(name);

  if(!desc) return cb(Errors.adapter.collectionNotRegistered);
  if(Object.keys(desc).length === 0) desc = null;

  // Hack needed for migrate: safe
  if(!this.definedCollections[collectionName]) desc = null;

  cb(null, desc);
};

/**
 * Define the schema for a collection.
 *
 * @param {String} collectionName
 * @param {Object} definition
 * @param {Function} callback
 * @api public
 */

Database.prototype.define = function define(collectionName, definition, cb) {
  var name = collectionName.toLowerCase();
  this.schema.registerCollection(name, definition);

  // Hack needed for migrate: safe to return nothing when described
  this.definedCollections[collectionName] = true;

  cb();
};

/**
 * Drop a collection and all of it's keys.
 *
 * @param {String} collectionName
 * @param {Array} relations
 * @param {Function} callback
 * @api public
 */

Database.prototype.drop = function drop(collectionName, relations, cb) {

   var self = this,
      name = collectionName.toLowerCase();

  var primary = this.schema._primary[name];
  var keyPrefix = this.schema.recordKey(name, primary, '');
  var keyPattern = keyPrefix  + '*';

  async.auto({
    keys: function (next) {
      self.connection.keys(keyPattern, next);
    },
    destroyAll: ['keys', function (next, results) {

      function destroyRecord(item, nextItem) {
        self.connection.del(item, nextItem);
      }

      async.each(results.keys, destroyRecord, next);
    }]
  }, function (err) {
    if (err) cb(err);
    cb();
  });

};

/**
 * Find a record or set of records based on a criteria object.
 *
 * @param {String} collectionName
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Database.prototype.find = function find(collectionName, criteria, cb) {
  var self = this,
      name = collectionName.toLowerCase(),
      recordKey;

  // Find the attribute used as a primary key
  var primary = this.schema._primary[name];


  // If the primary key is contained in the criteria and it is not an object/array), a NoSQL key can be
  // constructed and we can simply grab the values. This would be a findOne.
  // if the if statement is true then it can simply grab the record, but if it has more values than that then
  // it should do some more criteria solving, mainly for join's without unique id's and such
  if (
    criteria.where && criteria.where.hasOwnProperty(primary) &&
    !Array.isArray(criteria.where[primary]) &&
     Utils.present(criteria.where[primary]) &&
     Utils.count( criteria.where ) == 1
  ) {
      recordKey = this.schema.recordKey(name, primary, criteria.where[primary]);
      this.connection.get(recordKey, function(err, record) {
        if(err) return cb(err);
        if(!record) return cb(null, []);

        try {
          record = JSON.parse(record);
        } catch (e) {
          return cb(e);
        }

        cb(null, [self.schema.parse(name, record)]);
      });
    return;
  }

  cb(new Error('Please use primary key for the criteria, eg. find("PRIMARY-KEY")'));
};

/**
 * Create a new record from `data`
 *
 * @param {String} collectionName
 * @param {Object} data
 * @param {Function} callback
 * @api public
 */

Database.prototype.create = function create(collectionName, data, cb) {

  var self = this,
      name = collectionName.toLowerCase(),
      primary = self.schema._primary[name];

  name = Utils.replaceSpaces(name);

  // Grab the key to use for this record
  var recordKey = self.schema.recordKey(name, primary, data[primary]);

  recordKey = Utils.sanitize(recordKey);

  // add ttl support
  self._set(recordKey, data, function(err) {
    if(err) return cb(err);

    // Find the newly created record and return it
    self.connection.get(recordKey, function(err, values) {
      if(err) return cb(err);

      // Parse JSON into an object
      try {
        values = JSON.parse(values);
      } catch(e) {
        return cb(e);
      }

      cb(null, self.schema.parse(name, values));
    });
  });

};

/**
 * Update a record
 *
 * @param {String} collectionName
 * @param {Object} criteria
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */

Database.prototype.update = function update(collectionName, criteria, values, cb) {
  var self = this,
      name = collectionName.toLowerCase(),
      primary = self.schema._primary[name];


  // Don't allow the updating of primary keys
  if(Utils.present(values[primary]) && values[primary] !== criteria.where[primary]) {
    return cb(Errors.adapter.PrimaryKeyUpdate);
  }

  // Delete any primary keys
  delete values[primary];

  async.auto({

    // Find all matching records based on the criteria
    findRecords: function(next) {

      // Use the FIND method to get all the records that match the criteria
      self.find(name, criteria, function(err, records) {
        if(err) return next(err);
        next(null, records);
      });
    },

    // Update the record values
    updateRecords: ['findRecords', function(next, results) {
      var models = [];

      function updateRecord(item, nextItem) {
        var key = self.schema.recordKey(name, primary, item[primary]);
        var updatedValues = _.extend(item, values);

        self._set(key, updatedValues, function(err) {
          if(err) return nextItem(err);
          models.push(updatedValues);
          nextItem();
        });
      }

      async.each(results.findRecords, updateRecord, function(err) {
        if(err) return next(err);
        next(null, models);
      });
    }]

  }, function(err, results) {
    if(err) return cb(err);
    cb(null, results.updateRecords);
  });
};

/**
 * Destory Record(s)
 *
 * @param {String} collectionName
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Database.prototype.destroy = function destroy(collectionName, criteria, cb) {
  var self = this,
      name = collectionName.toLowerCase(),
      recordKey;

  // Find the attribute used as a primary key
  var primary = this.schema._primary[name];

  // If the primary key is contained in the criteria and it is not an object/array), a NoSQL key can be
  // constructed and we can simply grab the values. This would be a findOne.
  // if the if statement is true then it can simply grab the record, but if it has more values than that then
  // it should do some more criteria solving, mainly for join's without unique id's and such
  if (
    criteria.where && criteria.where.hasOwnProperty(primary) &&
    !Array.isArray(criteria.where[primary]) &&
     Utils.present(criteria.where[primary]) &&
     Utils.count( criteria.where ) == 1
  ) {
      recordKey = this.schema.recordKey(name, primary, criteria.where[primary]);
      this.connection.get(recordKey, function(err, record) {
        if(err) return cb(err);
        if(!record) return cb(null, []);

        self.connection.del(recordKey, function(err) {
          if(err) return cb(err);
          cb(null, record);
        });
      });
    return;
  }

  if (Utils.count( criteria.where ) == 0) {
    return self.drop(collectionName, '', cb);
  }

  cb(new Error('Please use primary key for the criteria, eg. destroy("PRIMARY-KEY") or destroy() to destroy all'));
};

////////////////////////////////////////////
// private methods
////////////////////////////////////////////

/**
 * set value to redis
 *
 * if data contained _ttl(time to live), _ttl will be the record expire time
 */

Database.prototype._set = function _set(recordKey, data, cb) {

  var ttl = +data._ttl; // cache ttl

  // Stringify data for storage
  var parsedData;
  try {
    parsedData = JSON.stringify(data);
  } catch(e) {
    return cb(e);
  }
  if (!ttl || ttl === -1) {
    this.connection.set(recordKey, parsedData, cb);
  } else {

    this.connection.setex(recordKey, ttl, parsedData, cb);
  }
}
