'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ElasticsearchStorageBackend = exports.ElasticsearchStorageHandler = undefined;

var _ElasticsearchStorageHandler2 = require('./entity/Elasticsearch-storage-handler');

var _ElasticsearchStorageHandler3 = _interopRequireDefault(_ElasticsearchStorageHandler2);

var _ElasticsearchStorage = require('./storage/Elasticsearch-storage');

var _ElasticsearchStorage2 = _interopRequireDefault(_ElasticsearchStorage);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

exports.ElasticsearchStorageHandler = _ElasticsearchStorageHandler3.default;
exports.ElasticsearchStorageBackend = _ElasticsearchStorage2.default;
exports.default = _ElasticsearchStorage2.default;