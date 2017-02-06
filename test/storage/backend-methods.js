import { ElasticsearchStorageBackend,
         ElasticsearchStorageHandler } from "./../../src/index"
import { EntityStorageHandler } from "entity-api"
import assert from "assert"
import equal from 'deep-equal'
import util from 'util'
import elasticsearch from 'elasticsearch'

const connectionParams = {
  host: "http://127.0.0.1:9200"
};

describe('Storage backend methods', function() {
  this.timeout(20000);

  describe('Test methods provided Elasticsearch storage backend.', () => {

    it('It should handle table prefixes', done => {
      let entityTypeId = 'test';
      let indexPrefix = 'indexPrefixTest_';

      let backend = new ElasticsearchStorageBackend({
        elasticsearch: new elasticsearch.Client(connectionParams)
      });
      let handler = new ElasticsearchStorageHandler({
        entityTypeId: entityTypeId,
        storage: backend,
        indexPrefix: indexPrefix
      });

      if (backend.getStorageIndexPrefix() != indexPrefix)
        return done(new Error("Storage backend table prefix was not expected"));

      if (backend.getStorageIndexName() != `${indexPrefix}${entityTypeId}`)
        return done(new Error("Storage backend table name was not prefixed as expected"));

      done();
    })
  });

});