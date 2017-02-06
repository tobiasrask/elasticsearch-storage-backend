import { ElasticsearchStorageBackend,
         ElasticsearchStorageHandler } from "./../../src/index"
import assert from "assert"
import equal from 'deep-equal'
import util from 'util'
import elasticsearch from 'elasticsearch'

const connectionParams = {
  host: "http://127.0.0.1:9200"
};

describe('Elasticsearch storage backend', function() {
  this.timeout(20000);

  describe('Invalid construction', () => {
    it('It should throw error if Elasticsearch instance is not provided', done => {
      try {
        let backend = new ElasticsearchStorageBackend();
      } catch (err) {
        return done();
      }
      done(new Error("It dint' throw error when Elasticsearch instance is missing"));
    })
  });

  describe('Construction', () => {
    it('Should construct with without errors', done => {
      class Elasticsearch {}
      let backend = new ElasticsearchStorageBackend({
        elasticsearch: new Elasticsearch()
      });
      done();
    })
  });

  describe('Load batch of items', () => {
    it('Should load large number of items as a patch', done => {
      let entityTypeId = 'test';

      class Elasticsearch {
        mget(params, callback) {
          let data = {
            _index: params.index,
            _type: params.type,
            docs: []
          };
          params.body.ids.forEach(entityId => {
            data.docs.push({
              _id: entityId,
              found: false
              });
          });
          callback(null, data);
        }
      };

      let backend = new ElasticsearchStorageBackend({
        elasticsearch: new Elasticsearch()
      });

      class CustomHandler extends ElasticsearchStorageHandler {
        getStorageIndexDefinitions() {
          return [{ fieldName: "entity_id" }];
        }
      }

      let handler = new CustomHandler({
        entityTypeId: entityTypeId,
        storage: backend
      });

      backend.loadEntityContainers([{ entity_id: 123 }], (err, result) => {
        if (err)
          return done(err);
        done();
      });
    })
  });

});
