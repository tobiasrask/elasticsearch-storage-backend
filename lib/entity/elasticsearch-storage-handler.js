'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _entityApi = require('entity-api');

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var ElasticsearchStorageHandler = function (_EntityStorageHandler) {
  _inherits(ElasticsearchStorageHandler, _EntityStorageHandler);

  /**
  * Construct
  */
  function ElasticsearchStorageHandler(variables) {
    _classCallCheck(this, ElasticsearchStorageHandler);

    // Apply index definitons for storage
    var _this = _possibleConstructorReturn(this, (ElasticsearchStorageHandler.__proto__ || Object.getPrototypeOf(ElasticsearchStorageHandler)).call(this, variables));

    if (variables.hasOwnProperty('storageIndexDefinitions')) _this._registry.set('properties', 'storageIndexDefinitions', variables.storageIndexDefinitions);

    if (variables.hasOwnProperty('schemaData')) _this._registry.set('properties', 'schemaData', variables.schemaData);

    if (variables.hasOwnProperty('indexPrefix')) _this._registry.set('properties', 'indexPrefix', variables.indexPrefix);
    return _this;
  }

  /**
  * Hook getStorageIndexDefinitions()
  */


  _createClass(ElasticsearchStorageHandler, [{
    key: 'getStorageIndexDefinitions',
    value: function getStorageIndexDefinitions() {
      return this._registry.get('properties', 'storageIndexDefinitions', []);
    }

    /**
    * Hook getIndices(). Index name relates to database name.
    */

  }, {
    key: 'getIndices',
    value: function getIndices() {
      var _this2 = this;

      return this._registry.get('properties', 'schemaData', { indices: [] }).indices.map(function (data) {
        if (!data.hasOwnProperty('name') || !data.name) data.indexName = _this2.getStorageIndexName();else data.indexName = _this2.getStorageIndexPrefix() + data.name;
        return data;
      }).filter(function (schema) {
        return schema != undefined;
      });
    }

    /**
    * Hook getSchemas() returns processed schema data.
    */

  }, {
    key: 'getSchemas',
    value: function getSchemas() {
      return {
        indices: this.getIndices(),
        mappings: this.getMappings()
      };
    }

    /**
    * Hook getSchemas()
    */

  }, {
    key: 'getMappings',
    value: function getMappings() {
      var _this3 = this;

      return this._registry.get('properties', 'schemaData', { mappings: [] }).mappings.map(function (mapping) {
        if (!mapping.hasOwnProperty('data')) return;

        var data = Object.assign({}, mapping);

        // Index defaults to entity type name
        if (!data.hasOwnProperty('index') || !data.index) data.indexName = _this3.getStorageIndexName();else data.indexName = _this3.getStorageIndexPrefix() + data.index;

        // Elastic type name defaults to entity type id...
        if (!data.hasOwnProperty('type') || !data.type) data.typeName = _this3.getStorageTypeName();else data.typeName = data.type;

        return data;
      }).filter(function (data) {
        return data != undefined;
      });
    }

    /**
    * Returns default index name for entity.
    */

  }, {
    key: 'getStorageIndexName',
    value: function getStorageIndexName() {
      return this.getStorageIndexPrefix() + this.getEntityTypeId();
    }

    /**
    * Returns index prefix for entity.
    */

  }, {
    key: 'getStorageIndexPrefix',
    value: function getStorageIndexPrefix() {
      return this._registry.get('properties', 'indexPrefix', '');
    }

    /**
    * Returns default mapping type name for entity.
    */

  }, {
    key: 'getStorageTypeName',
    value: function getStorageTypeName() {
      return this.getEntityTypeId();
    }
  }]);

  return ElasticsearchStorageHandler;
}(_entityApi.EntityStorageHandler);

exports.default = ElasticsearchStorageHandler;