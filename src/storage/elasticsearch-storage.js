import DomainMap from 'domain-map'
import EntitySystem, { StorageBackend } from "entity-api"

// TODO:
// Table status, install, update, uninstall
// Entity CRUDi operations

/**
* Simple memory storage backend.
*/
class ElasticsearchStorageBackend extends StorageBackend {

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
  constructor(variables = {}) {
    super(variables);

    if (variables.hasOwnProperty('lockUpdates'))
      this.setStorageLock(variables.lockUpdates);

    if (!variables.hasOwnProperty('elasticsearch'))
      throw new Error("Elasticsearch instance must be provided");

    this._registry.set("properties", 'elasticsearch', variables.elasticsearch);
  }

  /**
  * Returns assigned Elasticsearch Instance.
  *
  * @return instance
  */
  getElasticsearchInstance() {
    return this._registry.get("properties", 'elasticsearch');
  }

  /**
  * Load entity content container for entity data.
  *
  * @param id
  *   entity id data
  * @param callback
  */
  loadEntityContainer(entityId, callback) {
    var self = this;
    this.loadDataItem(this.getStorageIndexName(),
      this.getStorageTypeName(),
      this.prepareEntityId(entityId),
      callback);
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
  loadEntityContainers(entityIds, callback) {
    this.loadDataItems(this.getStorageIndexName(),
      this.getStorageTypeName(),
      entityIds,
      callback);
  }

  /**
  * Prepare entity id to be used with ES.
  *
  * @param entityId,
  * @return string entityId;
  */
  prepareEntityId(entityId) {
    return Buffer.from(JSON.stringify(entityId)).toString('base64');
  }

  /**
  * Decode entity id.
  *
  * @return data.
  */
  extractEntityId(id) {
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
  saveEntityContainer(entityId, container, callback) {
    this.saveDataItem(this.getStorageIndexName(),
      this.getStorageTypeName(),
      this.prepareEntityId(entityId),
      Object.assign({}, entityId, container),
      callback);
  }

  /**
  * Delete entity content container.
  *
  * @param entityId
  *   Entity id
  * @param callback
  */
  deleteEntityContainer(entityId, callback) {
    this.deleteDataItem(this.getStorageIndexName(),
      this.getStorageTypeName(),
      this.prepareEntityId(entityId),
      callback);
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
  saveDataItem(index, type, id, data, callback) {
    this.getElasticsearchInstance().update({
      index: index,
      type: type,
      id: id,
      doc_as_upsert: true,
      body: {
        doc: data
      }
    }, (err, result) => {
      // TODO: Handle throttling
      // TODO: Handle eventually consisency issues...
      callback(err, result);
    })
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
  loadDataItem(index, type, id, callback) {
    // TODO: Handle throttling
    // TODO: Handle eventually consisency issues...
    this.getElasticsearchInstance().get({
      index: index,
      type: type,
      id: id
    }, (err, resp) => {
      if (resp.found)
        callback(null, resp._source);
      else if (err && err.status == 404)
        callback(null, false);
      else
        callback(err);
    })
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
  deleteDataItem(index, type, id, callback) {
    this.getElasticsearchInstance().delete({
      index: index,
      type: type,
      id: id
    }, (err, result) => {
      callback(err, result);
    })
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
  loadDataItems(index, type, itemKeys, callback) {
    var self = this;
    let result = DomainMap.createCollection({ strictKeyMode: false });
    let countBatches = Math.ceil(itemKeys.length / maxItems);
    let indexeDefinitions = this.getStorageIndexDefinitions();
    let maxItems = 100;
    let pointer = 0;

    function loadBatchData(keys) {
      self.getElasticsearchInstance().mget({
        index: index,
        type: type,
        body: {
          ids: keys
        }
      }, (err, resp) => {
        if (err) return callback(err);

        resp.docs.forEach(item => {
          let entityId = self.extractEntityId(item._id);
          let data = item.found ? item._source : false;
          result.set(entityId, data);
        });
        getBatch();
      });
    }

    function getBatch() {
      let keys = [];
      if (pointer == itemKeys.length)
        return callback(null, result)
      while (pointer < itemKeys.length && keys.length < maxItems) {
        keys.push(self.prepareEntityId(itemKeys[pointer]));
        pointer++;
      }
      loadBatchData(keys)
    }

    getBatch();
  }

  /**
  * Build prefixed index name.
  *
  * @param indexName
  * @return prefixed index name
  */
  prefixedIndexName(indexName) {
    return this.getStorageIndexPrefix() + indexName;
  }

  /**
  * Return storege domain defined by storage handler.
  *
  * @return storage domain
  */
  getStorageIndexName() {
    return this.getStorageHandler().getStorageIndexName();
  }

  /**
  * Returns prefix for index defined by storage handler.
  *
  * @return storage domain
  */
  getStorageIndexPrefix() {
    return this.getStorageHandler().getStorageIndexPrefix();
  }


  /**
  * Return storage type name.
  *
  * @return storage domain
  */
  getStorageTypeName() {
    return this.getStorageHandler().getStorageTypeName();
  }

  /**
  * Return storege domain.
  *
  * @return storage domain
  */
  getStorageIndexDefinitions() {
    return this.getStorageHandler().getStorageIndexDefinitions();
  }

  /**
  * Select data from storage. If query doesn't contain index name, we will
  * populate index name to match entity specs.
  *
  * @param variables
  * @param callback
  */
  select(variables, callback) {
    this.executeSelectQuery(variables,
      this.buildSelectQuery(variables), callback);
  }

  /**
  * Build select parameters
  *
  * @param variables with following keys
  *   - query
  * @return params
  */
  buildSelectQuery(variables) {
    var self = this;
    let query = variables.hasOwnProperty('query') ? variables.query : {};

    // Check if we should fill index name
    if (variables.hasOwnProperty('index'))
      query.index = this.prefixedIndexName(variables.index);
    else if (!query.hasOwnProperty('index'))
      query.index = this.getStorageIndexName();

    if (variables.hasOwnProperty('fillTypeName'))
      query.type = this.getStorageTypeName();

    return query;
  }

  /**
  * Execute query
  *
  * @param variables
  * @param query
  * @param callback
  */
  executeSelectQuery(variables, query, callback) {
    var self = this;
    this.getElasticsearchInstance().search(query)
    .then(resp => {
      callback(null, resp);
    })
    .catch(err => {
      EntitySystem.log("ESStorageBackend", err.toString(), 'error');
      console.error(err);
      callback(err)
    });
  }

  /**
  * Returns list of Elasticsearch indexs installed.
  *
  * @param callback
  */
  getElasticsearchTables(callback) {
    let elasticsearch = this._registry.get("properties", 'elasticsearch');
    elasticsearch.listTables({}, function(err, data) {
      if (err) callback(err)
      else callback(null, data);
    });
  }

  /**
  * Returns list of DynamoDB indexs installed.
  *
  * @param callback
  */
  getElasticIndices(callback) {
    let elasticsearch = this._registry.get("properties", 'elasticsearch');
    elasticsearch.listTables({}, function(err, data) {
      if (err) callback(err)
      else callback(null, data);
    });
  }

  /**
  * Method checks if given index exists.
  *
  * @param indexName
  * @param callback
  */
  checkStorageTableStatus(indexName, callback) {
    this.getElasticIndices((err, result) => {
      if (err)
        return callback(err);
      else if (result.indexs.find(item => item == indexName))
        callback(null, true);
      else
        callback(null, false);
    })
  }

  /**
  * Install indice schemas
  *
  * @param scema
  *   Install one or more schemas
  * @param options
  * @param callback
  */
  installSchemas(schemas, options, callback) {
    let self = this;
    let errors = false;
    this.installIndices(schemas.indices)
    .then(() => {
      return this.installMappings(schemas.mappings);
    })
    .then(() => {
      setTimeout(() => {
        callback(null);
      }, 2000);
    })
    .catch(err => {
      callback(err);
    });
  }

  /**
  * Install indices.
  *
  * @param indices
  * @return promise
  */
  installIndices(indices = []) {
    let self = this;
    EntitySystem.log("ESStorageBackend", `Creating indices`);

    if (!indices.length)
      return Promise.resolve();

    return indices.reduce((sequence, indexData) => {
      return sequence.then(() => {
        EntitySystem.log("ESStorageBackend", `Creating index: ${indexData.title} ({indexData.indexName})`);

        // Allow alter for index data
        return self.prepareIndiceForInstall(indexData)
        .then(() => {
          return this.getStorageHandler().prepareIndiceForInstall(indexData)
        })
        .then(() => {
          return self.getElasticsearchInstance().indices.create({
            index: indexData.indexName,
            body: indexData.settings
          })
        })
        .then(result => {
          EntitySystem.log("ESStorageBackend", `Index created: ${indexData.title} (${indexData.indexName})`);
          return Promise.resolve();
        })
        .catch(err => {
          EntitySystem.log("ESStorageBackend", err.toString(), 'error');
          console.error(err);
          return Promise.resolve();
        });
      });
    }, Promise.resolve());
  }

  /**
  * Update indices settings.
  *
  * @param index
  * @param options
  * @return promise
  */
  updateIndices(indices = [], options = {}) {
    let self = this;
    EntitySystem.log("ESStorageBackend", `Udate indices`);

    if (!indices.length)
      return Promise.resolve();

    return indices.reduce((sequence, indexData) => {
      return sequence.then(() => {
        EntitySystem.log("ESStorageBackend", `Updating index: ${indexData.title} ({indexData.indexName})`);

        // Allow alter for index data
        return self.prepareIndiceForUpdate(indexData)
        .then(() => {
          return this.getStorageHandler().prepareIndiceForUpdate(indexData)
        })
        .then(() => {
          EntitySystem.log("ESStorageBackend", `Closing index: ${indexData.title} ({indexData.indexName})`);
          return self.getElasticsearchInstance().indices.close({
            index: indexData.indexName
          })
        })
        .then(() => {
          EntitySystem.log("ESStorageBackend", `Perform update for index: ${indexData.title} ({indexData.indexName})`);
          return self.getElasticsearchInstance().indices.putSettings({
            index: indexData.indexName,
            body: indexData.settings
          })
        })
        .then(() => {
          EntitySystem.log("ESStorageBackend", `Opening index: ${indexData.title} ({indexData.indexName})`);
          return self.getElasticsearchInstance().indices.open({
            index: indexData.indexName
          })
        })
        .then(result => {
          EntitySystem.log("ESStorageBackend", `Index updated: ${indexData.title} (${indexData.indexName})`);
          return Promise.resolve();
        })
        .catch(err => {
          EntitySystem.log("ESStorageBackend", err.toString(), 'error');
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
  prepareIndiceForInstall(indexData) {
    return Promise.resolve();
  }

  /**
  * Hook to prepare index data to be updated. This allows us to fork and apply
  * data for indice before it will be installed.
  *
  * @param indexData
  * @return promise
  */
  prepareIndiceForUpdate(indexData) {
    return Promise.resolve();
  }

  /**
  * Install mappings.
  *
  * @param mappings
  * @return promise
  */
  installMappings(mappings = []) {
    let self = this;
    EntitySystem.log("ESStorageBackend", `Creating mappings`);

    if (!mappings.length)
      return Promise.resolve();

    return mappings.reduce((sequence, mappingData) => {
      return sequence.then(() => {
        EntitySystem.log("ESStorageBackend", `Creating mapping: ${mappingData.title} ({mappingData.typeName})`);

        let body = {};
        body[mappingData.typeName] = mappingData.data;

        return self.getElasticsearchInstance().indices.putMapping({
            index: mappingData.indexName,
            type: mappingData.typeName,
            update_all_types: true,
            body: body
        })
        .then(result => {
          EntitySystem.log("ESStorageBackend", `Mapping created: ${mappingData.title} (${mappingData.typeName})`);
          return Promise.resolve();
        })
        .catch(err => {
          EntitySystem.log("ESStorageBackend", err.toString(), 'error');
          throw new Error("Unable to create mapping");
        });
      });
    }, Promise.resolve());
  }

  /**
  * Update indice schemas
  *
  * @param scema
  *   Install one or more schemas
  * @param options
  * @param callback
  */
  updateSchemas(schemas, options, callback) {
    let self = this;
    let errors = false;
    this.updateIndices(schemas.indices)
    .then(() => {
      callback(null);
    })
    .catch(err => {
      callback(err);
    });
  }

  /**
  * Uninstall schema
  *
  * @param scema
  *   Install one or more schemas
  * @param options
  * @param callback
  */
  uninstallSchemas(schemas, options, callback) {
    let self = this;
    EntitySystem.log("ESStorageBackend", `Deleting indices`);

    if (!schemas.indices.length)
      return callback(null);

    schemas.indices.reduce((sequence, indexData) => {
      return sequence.then(() => {
        EntitySystem.log("ESStorageBackend", `Deleting index: ${indexData.title} ({indexData.indexName})`);
        return self.getElasticsearchInstance().indices.delete({
            index: indexData.indexName
        })
        .then(result => {
          EntitySystem.log("ESStorageBackend", `Index deleted: ${indexData.title} (${indexData.indexName})`);
          return Promise.resolve(true);
        })
        .catch(err => {
          EntitySystem.log("ESStorageBackend", err.toString(), 'error');
          return Promise.resolve(false);
        });
      });
    }, Promise.resolve())
    .then(() => {
      setTimeout(() => {
        callback(null);
      }, 3000);
    })
    .catch(err => {
      callback(err);
    });
  }
}

export default ElasticsearchStorageBackend;