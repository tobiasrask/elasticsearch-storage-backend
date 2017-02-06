import { ElasticsearchStorageHandler } from "./../../src/index"
import assert from "assert"
import equal from 'deep-equal'
import util from 'util'
import elasticsearch from 'elasticsearch'

const connectionParams = {
  host: "http://127.0.0.1:9200"
};


describe('Storage handler', function() {
  this.timeout(20000);

  describe('Test methods provided by Elasticsearch storage handler.', () => {
    it('It should handle schemas provided index name', done => {
      let erros = [];
      let entityTypeId = 'entityTypeId';
      let indexKey = 'indexKey';
      let indexPrefix = 'indexPrefix';

      let handler = new ElasticsearchStorageHandler({
        entityTypeId: entityTypeId,
        indexPrefix: indexPrefix,
        schemaData: {
          indices: [
            {
              title: "Test indice",
              description: "Test indice",
              name: `${indexKey}_row_0`,
              settings: {}
            },
            {
              title: "Test indice",
              description: "Test indice",
              name: `${indexKey}_row_1`,
              settings: {}
            }
          ],
          mappings: []
        }
      });

      if (handler.getStorageIndexPrefix() != indexPrefix)
        return done(new Error("Storage backend table prefix was not expected"));

      if (handler.getStorageIndexName() != `${indexPrefix}${entityTypeId}`)
        return done(new Error("Storage backend table name was not prefixed as expected"));

      let indices = handler.getIndices();

      if (!indices ||indices.length != 2)
        return done(new Error("Schema length doesn't match"));

      indices.forEach((indice, index) => {
        if (indice.indexName != `${indexPrefix}${indexKey}_row_${index}`)
          erros.push(new Error("Index name is not expected."));

        if (indice.name != `${indexKey}_row_${index}`)
          erros.push(new Error("Index name key doesn't match."));
      });

      if (erros.length)
        done(erros.pop());
      else
        done();
    })
  });

  describe('Test methods provided by Elasticsearch storage handler.', () => {
    it('It should handle schemas with default index name', done => {
      let erros = [];
      let entityTypeId = 'entityTypeId';
      let indexKey = 'indexKey';
      let indexPrefix = 'indexPrefix';

      let handler = new ElasticsearchStorageHandler({
        entityTypeId: entityTypeId,
        indexPrefix: indexPrefix,
        schemaData: {
          indices: [
            {
              title: "Test indice",
              description: "Test indice",
              settings: {}
            },
            {
              title: "Test indice",
              description: "Test indice",
              settings: {}
            }
          ],
          mappings: []
        }
      });

      if (handler.getStorageIndexPrefix() != indexPrefix)
        return done(new Error("Storage backend table prefix was not expected"));

      if (handler.getStorageIndexName() != `${indexPrefix}${entityTypeId}`)
        return done(new Error("Storage backend table name was not prefixed as expected"));

      let indices = handler.getIndices();

      if (!indices ||indices.length != 2)
        return done(new Error("Schema length doesn't match"));

      indices.forEach((indice, index) => {
        if (indice.indexName != `${indexPrefix}${entityTypeId}`)
          erros.push(new Error("Index name is not expected."));
      });

      if (erros.length)
        done(erros.pop());
      else
        done();
    })
  });

});