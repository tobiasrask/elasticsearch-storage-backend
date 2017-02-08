import { EntityStorageHandler } from "entity-api"

class ElasticsearchStorageHandler extends EntityStorageHandler {

  /**
  * Construct
  */
  constructor(variables) {
    super(variables);

    // Apply index definitons for storage
    if (variables.hasOwnProperty('storageIndexDefinitions'))
      this._registry.set('properties', 'storageIndexDefinitions', variables.storageIndexDefinitions);

   if (variables.hasOwnProperty('schemaData'))
      this._registry.set('properties', 'schemaData', variables.schemaData);

   if (variables.hasOwnProperty('indexPrefix'))
      this._registry.set('properties', 'indexPrefix', variables.indexPrefix);

   if (variables.hasOwnProperty('indexName'))
      this._registry.set('properties', 'indexName', variables.indexName);

  }

  /**
  * Hook getStorageIndexDefinitions()
  */
  getStorageIndexDefinitions() {
    return this._registry.get('properties', 'storageIndexDefinitions', []);
  }

  /**
  * Hook getIndices(). Index name relates to database name.
  */
  getIndices() {
    return this._registry.get('properties', 'schemaData', { indices: [] })
    .indices.map(data => {
      if (!data.hasOwnProperty('name') || !data.name)
        data.indexName = this.getStorageIndexName();
      else
        data.indexName = this.getStorageIndexPrefix() + data.name;
      return data;
    }).filter(schema => schema != undefined);
  }

  /**
  * Hook getSchemas() returns processed schema data.
  */
  getSchemas() {
    return {
      indices: this.getIndices(),
      mappings: this.getMappings()
    };
  }

  /**
  * Hook getSchemas()
  */
  getMappings() {
    return this._registry.get('properties', 'schemaData', { mappings: [] })
    .mappings.map(mapping => {
      if (!mapping.hasOwnProperty('data'))
        return;

      let data = Object.assign({}, mapping);

      // Index defaults to entity type name
      if (!data.hasOwnProperty('index') || !data.index)
        data.indexName = this.getStorageIndexName();
      else
        data.indexName = this.getStorageIndexPrefix() + data.index;

      // Elastic type name defaults to entity type id...
      if (!data.hasOwnProperty('type') || !data.type)
        data.typeName = this.getStorageTypeName();
      else
        data.typeName = data.type;

      return data;
    }).filter(data => data != undefined);
  }

  /**
  * Returns default index name for entity.
  *
  * @return index name
  *   Defaults to entity type id
  */
  getStorageIndexName() {
    return this.getStorageIndexPrefix() +
      this._registry.get('properties', 'indexName', this.getEntityTypeId());
  }

  /**
  * Returns index prefix for entity.
  */
  getStorageIndexPrefix() {
    return this._registry.get('properties', 'indexPrefix', '');
  }

  /**
  * Returns default mapping type name for entity.
  */
  getStorageTypeName() {
    return this.getEntityTypeId();
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
}

export default ElasticsearchStorageHandler;