import { ElasticsearchStorageBackend,
         ElasticsearchStorageHandler } from "./../../src/index"
import assert from "assert"
import equal from 'deep-equal'
import util from 'util'
import elasticsearch from 'elasticsearch'

const DB_SCHEMAS = {
  indices: [
    {
      title: "Test indice",
      description: "Test indice",
      settings: {}
    }
  ],
  mappings: [
    {
      title: "Test mapping",
      description: 'First test mapping',
      type: "testmapping1",
      data: {
        properties: {
          event_id: {
            "type": "string",
            "index": "not_analyzed"
          }
        }
      }
    },
    {
      title: "Test mapping",
      description: 'Second test mapping',
      type: "testmapping2",
      data: {
        properties: {
          event_id: {
            "type": "string",
            "index": "not_analyzed"
          }
        }
      }
    }
  ]
}

const connectionParams = {
  host: "http://127.0.0.1:9200"
};

class CustomStorageHandler extends ElasticsearchStorageHandler {
  constructor(variables) {
    variables.schemaData = DB_SCHEMAS;
    super(variables)
  }
  getStorageIndexName() {
    return 'schema_test';
  }
  getStorageIndexDefinitions() {
    return [{ fieldName: "entity_id" }];
  }
}

let storageBackend = null;

describe('Elasticsearch schema maneuvers', function() {
  this.timeout(20000);

  before(function() {
    // Create storage backend
    storageBackend = new ElasticsearchStorageBackend({
      elasticsearch: new elasticsearch.Client(Object.assign(connectionParams))
    });
  });

  describe('Schema uninstallation', () => {
    it('It should uninstall schemas if exists', done => {
      let entityTypeId = 'test';
      let handler = new CustomStorageHandler({
        entityTypeId: entityTypeId,
        storage: storageBackend
      });
      handler.uninstall()
      .then(result => {
        done();
      })
      .catch(done);
    })
  });

  describe('Schema installation', () => {
    it('It should install schemas', done => {
      let entityTypeId = 'test';
      let handler = new CustomStorageHandler({
        entityTypeId: entityTypeId,
        storage: storageBackend
      });
      handler.install()
      .then(result => {
        done();
      })
      .catch(done);
    })
  });

  describe('Schema storage', () => {
    it('It should save, load and delete data from schema tables', done => {
      let entityTypeId = 'test';
      let handler = new CustomStorageHandler({
        entityTypeId: entityTypeId,
        storage: storageBackend
      });
      let itemKey = {
        entity_id: '12345'
      }
      let fields = {
        fieldA: '123',
        fieldB: '456',
        fieldC: '789'
      }
      let testItem = Object.assign({}, itemKey, fields);

      storageBackend.saveEntityContainer(itemKey, fields, err => {
        if (err)
          return done(err);

        storageBackend.loadEntityContainer(itemKey, (err, container) => {
          if (err)
            return done(err);

          if (!equal(testItem, container))
            return done("Loaded Elasticsearch container is not expected.");

          storageBackend.deleteEntityContainer(itemKey, (err, result)=> {
            if (err)
              return done(err);

            storageBackend.loadEntityContainer(itemKey, (err, container) => {
              if (err)
                return done(err);

              if (container)
                done(new Error("It didn't delete entity container"));
              else
                done();
            });
          })
        });
      });
    })
  });

  describe('Schema uninstallation', () => {
    it('It should uninstall schemas', done => {
      let entityTypeId = 'test';
      let handler = new CustomStorageHandler({
        entityTypeId: entityTypeId,
        storage: storageBackend
      });
      handler.uninstall()
      .then(result => {
        done();
      })
      .catch(done);
    })
  });
});