'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _domainMap = require('domain-map');

var _domainMap2 = _interopRequireDefault(_domainMap);

var _entityApi = require('entity-api');

var _entityApi2 = _interopRequireDefault(_entityApi);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

// TODO:
// Table status, install, update, uninstall
// Entity CRUDi operations

/**
* Simple memory storage backend.
*/
var ElasticsearchStorageBackend = function (_StorageBackend) {
  _inherits(ElasticsearchStorageBackend, _StorageBackend);

  /**
  * Construct
  *
  * @param variables with following keys:
  *   storageHandler
  *     Storage handler who is using storage backend.
  *   elasticsearch
  *     Elasticsearch instance configued by client:
  *     http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Elasticsearch.html
  */
  function ElasticsearchStorageBackend() {
    var variables = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};

    _classCallCheck(this, ElasticsearchStorageBackend);

    var _this = _possibleConstructorReturn(this, (ElasticsearchStorageBackend.__proto__ || Object.getPrototypeOf(ElasticsearchStorageBackend)).call(this, variables));

    if (variables.hasOwnProperty('lockUpdates')) _this.setStorageLock(variables.lockUpdates);

    if (!variables.hasOwnProperty('elasticsearch')) throw new Error("Elasticsearch instance must be provided");

    _this._registry.set("properties", 'elasticsearch', variables.elasticsearch);
    return _this;
  }

  /**
  * Returns assigned Elasticsearch Instance.
  *
  * @return instance
  */


  _createClass(ElasticsearchStorageBackend, [{
    key: 'getElasticsearchInstance',
    value: function getElasticsearchInstance() {
      return this._registry.get("properties", 'elasticsearch');
    }

    /**
    * Load entity content container for entity data.
    *
    * @param id
    *   entity id data
    * @param callback
    */

  }, {
    key: 'loadEntityContainer',
    value: function loadEntityContainer(entityId, callback) {
      var self = this;
      this.loadDataItem(this.getStorageIndexName(), this.getStorageTypeName(), this.prepareEntityId(entityId), callback);
    }

    /**
    * Load entity content container for entity data. Elasticsearch supports only
    * fetching items in batches size of 100 items.
    *
    * TODO: Support throttling
    *
    * @param keys
    *   Array of entity ids.
    * @param callback
    */

  }, {
    key: 'loadEntityContainers',
    value: function loadEntityContainers(entityIds, callback) {
      this.loadDataItems(this.getStorageIndexName(), this.getStorageTypeName(), entityIds, callback);
    }

    /**
    * Prepare entity id to be used with ES.
    *
    * @param entityId,
    * @return string entityId;
    */

  }, {
    key: 'prepareEntityId',
    value: function prepareEntityId(entityId) {
      return Buffer.from(JSON.stringify(entityId)).toString('base64');
    }

    /**
    * Decode entity id.
    *
    * @return data.
    */

  }, {
    key: 'extractEntityId',
    value: function extractEntityId(id) {
      return JSON.parse(Buffer.from(id, 'base64').toString('ascii'));
    }

    /**
    * Save entity content container.
    *
    * @param entityId
    *   Entity id
    * @param container
    *   Container data
    * @param caallback
    */

  }, {
    key: 'saveEntityContainer',
    value: function saveEntityContainer(entityId, container, callback) {
      this.saveDataItem(this.getStorageIndexName(), this.getStorageTypeName(), this.prepareEntityId(entityId), Object.assign({}, entityId, container), callback);
    }

    /**
    * Delete entity content container.
    *
    * @param entityId
    *   Entity id
    * @param callback
    */

  }, {
    key: 'deleteEntityContainer',
    value: function deleteEntityContainer(entityId, callback) {
      this.deleteDataItem(this.getStorageIndexName(), this.getStorageTypeName(), this.prepareEntityId(entityId), callback);
    }

    /**
    * Save data to Elasticsearch.
    *
    * @param index
    *   Index name
    * @param type
    *   Mapping type
    * @param id
    *   Entity id
    * @param data
    * @param callback
    */

  }, {
    key: 'saveDataItem',
    value: function saveDataItem(index, type, id, data, callback) {
      this.getElasticsearchInstance().update({
        index: index,
        type: type,
        id: id,
        doc_as_upsert: true,
        body: {
          doc: data
        }
      }, function (err, result) {
        // TODO: Handle throttling
        // TODO: Handle eventually consisency issues...
        callback(err, result);
      });
    }

    /**
    * Load data items.
    *
    * @param index
    *   Index name
    * @param type
    *   Mapping type
    * @param id
    *   Entity id
    * @param callback
    */

  }, {
    key: 'loadDataItem',
    value: function loadDataItem(index, type, id, callback) {
      // TODO: Handle throttling
      // TODO: Handle eventually consisency issues...
      this.getElasticsearchInstance().get({
        index: index,
        type: type,
        id: id
      }, function (err, resp) {
        if (resp.found) callback(null, resp._source);else if (err && err.status == 404) callback(null, false);else callback(err);
      });
    }

    /**
    * Delete data item.
    *
    * @param index
    *   Index name
    * @param type
    *   Mapping type
    * @param id
    *   Entity id
    * @param callback
    */

  }, {
    key: 'deleteDataItem',
    value: function deleteDataItem(index, type, id, callback) {
      this.getElasticsearchInstance().delete({
        index: index,
        type: type,
        id: id
      }, function (err, result) {
        callback(err, result);
      });
    }

    /**
    * Load batch of data items.
    *
    * @param index
    * @param ids
    *   Array of key data
    * @param callback
    *   Data collection
    */

  }, {
    key: 'loadDataItems',
    value: function loadDataItems(index, type, itemKeys, callback) {
      var self = this;
      var result = _domainMap2.default.createCollection({ strictKeyMode: false });
      var countBatches = Math.ceil(itemKeys.length / maxItems);
      var indexeDefinitions = this.getStorageIndexDefinitions();
      var maxItems = 100;
      var pointer = 0;

      function loadBatchData(keys) {
        self.getElasticsearchInstance().mget({
          index: index,
          type: type,
          body: {
            ids: keys
          }
        }, function (err, resp) {
          if (err) return callback(err);

          resp.docs.forEach(function (item) {
            var entityId = self.extractEntityId(item._id);
            var data = item.found ? item._source : false;
            result.set(entityId, data);
          });
          getBatch();
        });
      }

      function getBatch() {
        var keys = [];
        if (pointer == itemKeys.length) return callback(null, result);
        while (pointer < itemKeys.length && keys.length < maxItems) {
          keys.push(self.prepareEntityId(itemKeys[pointer]));
          pointer++;
        }
        loadBatchData(keys);
      }

      getBatch();
    }

    /**
    * Build prefixed index name.
    *
    * @param indexName
    * @return prefixed index name
    */

  }, {
    key: 'prefixedIndexName',
    value: function prefixedIndexName(indexName) {
      return this.getStorageIndexPrefix() + indexName;
    }

    /**
    * Return storege domain defined by storage handler.
    *
    * @return storage domain
    */

  }, {
    key: 'getStorageIndexName',
    value: function getStorageIndexName() {
      return this.getStorageHandler().getStorageIndexName();
    }

    /**
    * Returns prefix for index defined by storage handler.
    *
    * @return storage domain
    */

  }, {
    key: 'getStorageIndexPrefix',
    value: function getStorageIndexPrefix() {
      return this.getStorageHandler().getStorageIndexPrefix();
    }

    /**
    * Return storage type name.
    *
    * @return storage domain
    */

  }, {
    key: 'getStorageTypeName',
    value: function getStorageTypeName() {
      return this.getStorageHandler().getStorageTypeName();
    }

    /**
    * Return storege domain.
    *
    * @return storage domain
    */

  }, {
    key: 'getStorageIndexDefinitions',
    value: function getStorageIndexDefinitions() {
      return this.getStorageHandler().getStorageIndexDefinitions();
    }

    /**
    * Select data from storage. If query doesn't contain index name, we will
    * populate index name to match entity specs.
    *
    * @param variables
    * @param callback
    */

  }, {
    key: 'select',
    value: function select(variables, callback) {
      this.executeSelectQuery(variables, this.buildSelectQuery(variables), callback);
    }

    /**
    * Build select parameters
    *
    * @param variables with following keys
    *   - query
    * @return params
    */

  }, {
    key: 'buildSelectQuery',
    value: function buildSelectQuery(variables) {
      var self = this;
      var query = variables.hasOwnProperty('query') ? variables.query : {};

      // Check if we should fill index name
      if (variables.hasOwnProperty('index')) query.index = this.prefixedIndexName(variables.index);else if (!query.hasOwnProperty('index')) query.index = this.getStorageIndexName();

      if (variables.hasOwnProperty('fillTypeName')) query.type = this.getStorageTypeName();

      return query;
    }

    /**
    * Execute query
    *
    * @param variables
    * @param query
    * @param callback
    */

  }, {
    key: 'executeSelectQuery',
    value: function executeSelectQuery(variables, query, callback) {
      var self = this;
      this.getElasticsearchInstance().search(query).then(function (resp) {
        callback(null, resp);
      }).catch(function (err) {
        _entityApi2.default.log("ESStorageBackend", err.toString(), 'error');
        console.error(err);
        callback(err);
      });
    }

    /**
    * Returns list of Elasticsearch indexs installed.
    *
    * @param callback
    */

  }, {
    key: 'getElasticsearchTables',
    value: function getElasticsearchTables(callback) {
      var elasticsearch = this._registry.get("properties", 'elasticsearch');
      elasticsearch.listTables({}, function (err, data) {
        if (err) callback(err);else callback(null, data);
      });
    }

    /**
    * Returns list of DynamoDB indexs installed.
    *
    * @param callback
    */

  }, {
    key: 'getElasticIndices',
    value: function getElasticIndices(callback) {
      var elasticsearch = this._registry.get("properties", 'elasticsearch');
      elasticsearch.listTables({}, function (err, data) {
        if (err) callback(err);else callback(null, data);
      });
    }

    /**
    * Method checks if given index exists.
    *
    * @param indexName
    * @param callback
    */

  }, {
    key: 'checkStorageTableStatus',
    value: function checkStorageTableStatus(indexName, callback) {
      this.getElasticIndices(function (err, result) {
        if (err) return callback(err);else if (result.indexs.find(function (item) {
          return item == indexName;
        })) callback(null, true);else callback(null, false);
      });
    }

    /**
    * Install indice schemas
    *
    * @param scema
    *   Install one or more schemas
    * @param options
    * @param callback
    */

  }, {
    key: 'installSchemas',
    value: function installSchemas(schemas, options, callback) {
      var _this2 = this;

      var self = this;
      var errors = false;

      // Create indices and mappings
      this.installIndices(schemas.indices).then(function () {
        return _this2.installMappings(schemas.mappings);
      }).then(function () {
        setTimeout(function () {
          callback(null);
        }, 2000);
      }).catch(function (err) {
        callback(err);
      });
    }

    /**
    * Install indices.
    *
    * @param indices
    * @return promise
    */

  }, {
    key: 'installIndices',
    value: function installIndices() {
      var _this3 = this;

      var indices = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : [];

      var self = this;
      _entityApi2.default.log("ESStorageBackend", 'Creating indices');

      if (!indices.length) return Promise.resolve();

      return indices.reduce(function (sequence, indexData) {
        return sequence.then(function () {
          _entityApi2.default.log("ESStorageBackend", 'Creating index: ' + indexData.title + ' ({indexData.indexName})');

          // Allow alter for index data
          return self.prepareIndiceForInstall(indexData).then(function () {
            return _this3.getStorageHandler().prepareIndiceForInstall(indexData);
          }).then(function () {
            return self.getElasticsearchInstance().indices.create({
              index: indexData.indexName,
              body: indexData.settings
            });
          }).then(function (result) {
            _entityApi2.default.log("ESStorageBackend", 'Index created: ' + indexData.title + ' (' + indexData.indexName + ')');
            return Promise.resolve();
          }).catch(function (err) {
            _entityApi2.default.log("ESStorageBackend", err.toString(), 'error');
            console.error(err);
            return Promise.resolve();
          });
        });
      }, Promise.resolve());
    }

    /**
    * Hook to prepare index data to be installed. This allows us to fork and apply
    * data for indice before it will be installed.
    *
    * @param indexData
    * @return promise
    */

  }, {
    key: 'prepareIndiceForInstall',
    value: function prepareIndiceForInstall(indexData) {
      return Promise.resolve();
    }

    /**
    * Install mappings.
    *
    * @param mappings
    * @return promise
    */

  }, {
    key: 'installMappings',
    value: function installMappings() {
      var mappings = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : [];

      var self = this;
      _entityApi2.default.log("ESStorageBackend", 'Creating mappings');

      if (!mappings.length) return Promise.resolve();

      return mappings.reduce(function (sequence, mappingData) {
        return sequence.then(function () {
          _entityApi2.default.log("ESStorageBackend", 'Creating mapping: ' + mappingData.title + ' ({mappingData.typeName})');

          var body = {};
          body[mappingData.typeName] = mappingData.data;

          return self.getElasticsearchInstance().indices.putMapping({
            index: mappingData.indexName,
            type: mappingData.typeName,
            update_all_types: true,
            body: body
          }).then(function (result) {
            _entityApi2.default.log("ESStorageBackend", 'Mapping created: ' + mappingData.title + ' (' + mappingData.typeName + ')');
            return Promise.resolve();
          }).catch(function (err) {
            _entityApi2.default.log("ESStorageBackend", err.toString(), 'error');
            throw new Error("Unable to create mapping");
          });
        });
      }, Promise.resolve());
    }

    /**
    * Update schema
    *
    * @param scema
    *   Install one or more schemas
    * @param options
    * @param callback
    */

  }, {
    key: 'updateSchemas',
    value: function updateSchemas(schemas, options, callback) {
      callback(null);
    }

    /**
    * Uninstall schema
    *
    * @param scema
    *   Install one or more schemas
    * @param options
    * @param callback
    */

  }, {
    key: 'uninstallSchemas',
    value: function uninstallSchemas(schemas, options, callback) {
      var self = this;
      _entityApi2.default.log("ESStorageBackend", 'Deleting indices');

      if (!schemas.indices.length) return callback(null);

      schemas.indices.reduce(function (sequence, indexData) {
        return sequence.then(function () {
          _entityApi2.default.log("ESStorageBackend", 'Deleting index: ' + indexData.title + ' ({indexData.indexName})');
          return self.getElasticsearchInstance().indices.delete({
            index: indexData.indexName
          }).then(function (result) {
            _entityApi2.default.log("ESStorageBackend", 'Index deleted: ' + indexData.title + ' (' + indexData.indexName + ')');
            return Promise.resolve(true);
          }).catch(function (err) {
            _entityApi2.default.log("ESStorageBackend", err.toString(), 'error');
            return Promise.resolve(false);
          });
        });
      }, Promise.resolve()).then(function () {
        setTimeout(function () {
          callback(null);
        }, 3000);
      }).catch(function (err) {
        callback(err);
      });
    }
  }]);

  return ElasticsearchStorageBackend;
}(_entityApi.StorageBackend);

exports.default = ElasticsearchStorageBackend;