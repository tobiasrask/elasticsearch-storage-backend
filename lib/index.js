'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ElasticsearchStorageBackend = exports.ElasticsearchStorageHandler = undefined;

var _elasticsearchStorageHandler = require('./entity/elasticsearch-storage-handler');

var _elasticsearchStorageHandler2 = _interopRequireDefault(_elasticsearchStorageHandler);

var _elasticsearchStorage = require('./storage/elasticsearch-storage');

var _elasticsearchStorage2 = _interopRequireDefault(_elasticsearchStorage);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

exports.ElasticsearchStorageHandler = _elasticsearchStorageHandler2.default;
exports.ElasticsearchStorageBackend = _elasticsearchStorage2.default;
exports.default = _elasticsearchStorage2.default;