import { ElasticsearchStorageBackend,
         ElasticsearchStorageHandler } from "./../../src/index"
import assert from "assert"
import equal from 'deep-equal'
import util from 'util'
import elasticsearch from 'elasticsearch'

const entityTypeId = 'node';
const indexName = 'entity';

const DB_SCHEMAS = {
  indices: [
    {
      title: "Test entity",
      description: "Test entity",
      name: indexName,
      settings: {}
    }
  ],
  mappings: [
    {
      title: "Test mapping",
      description: 'First test mapping',
      type: "test",
      index: "entity",
      data: {
        properties: {
          event_id: {
            "type": "string",
            "index": "not_analyzed"
          },
          test_id: {
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

// Our test entity
// TODO: Make generic version

let backend = null;
let handler = null;

describe('Elasticsearch as entity api backend', function() {
  this.timeout(20000);

  before(function() {
    // Create storage backend
    backend = new ElasticsearchStorageBackend({
      elasticsearch: new elasticsearch.Client(Object.assign(connectionParams))
    });
    handler = new ElasticsearchStorageHandler({
      entityTypeId: entityTypeId,
      indexName: indexName,
      storage: backend,
      schemaData: DB_SCHEMAS
    });
  });

  describe('Schema uninstallation', () => {
    it('It should uninstall schemas if exists', done => {
      handler.uninstall()
      .then(result => {
        done();
      })
      .catch(done);
    })
  });

  describe('Schema installation', () => {
    it('It should install schemas', done => {
      handler.install()
      .then(result => {
        done();
      })
      .catch(done);
    })
  });

  describe('loadEntityContainers', () => {
    it('Should load large number of items as a patch', done => {
      backend.loadEntityContainers([{ entity_id: 123 }], (err, result) => {
        if (err)
          return done(err);
        done();
      });
    })
  });

  describe('Schema storage', () => {
    it('It should return saved entities', done => {
      let entities = [
        {
          id: { entity_id: 'entity1' },
          fields:  {
            name: "Bob"
          }
        },
        {
          id: { entity_id: 'entity2' },
          fields:  {
            name: "Marley"
          }
        },
        {
          id: { entity_id: 'entity3' },
          fields:  {
            name: "Harley"
          }
        }
      ];

      entities.reduce((sequence, entityData) => {
        return sequence.then(() => {
          return new Promise((resolve, reject) => {
            backend.saveEntityContainer(
              entityData.id,
              entityData.fields,
              err => {
              if (err) reject(err);
              else resolve();
            });
          });
        });
      }, Promise.resolve())
      .then(() => {
        return new Promise((resolve, reject) => {
          backend.loadEntityContainers(entities.map(i => i.id),
            (err, result) => {
            if (err) reject(err);
            else resolve(result.getMap());
          });
        });
      })
      .then(result => {
        if (result.size != 3)
          throw new Error("Result size is not expected");
        done();
      })
      .catch(done);
    })
  });

  describe('Search & select', () => {
    it('Should load perform serach and return data', done => {
      let params = {
        fillTypeName: true,
        query: { match_all: {} }
      };
      backend.select(params, (err, result) => {
        if (err)
          return done(err);

        if (!params.query.hasOwnProperty('index') ||
             params.query.index != indexName)
          return done(new Error("It didn't fill index name for select query"));

        if (!params.query.hasOwnProperty('type') ||
             params.query.type != entityTypeId)
          return done(new Error("It didn't fill type name for select query"));

        done();
      });
    })
  });

});
