var stream = require('stream')
  , assert = require('assert')
  , request = require('request')
  , sinon = require('sinon')
  , CKAN = require('ckan')
  , Importer = require('../index.js').Importer
  ;

function makeStream(string) {
  var s = new stream.Readable();
  s.push(string);
  s.push(null);
  return s;
}

describe('', function() {
  var stubbed = null;
  before(function() {
    stubbed = sinon.stub(CKAN.Client.prototype, '_ajax', function(arg1, cb) {
      cb(null, 'data');
    });
  });
  after(function() {
    // stubbed.restore();
  });
  it('works ok', function(done) {
    var importer = new Importer('http://localhost');
    var inStream = 'A,B\n1,2';
    var resource = {
      name: 'xyz',
      id: '123-xyz',
      schema: {
        fields: [
          {
            name: 'A',
            type: 'integer'
          },
          {
            name: 'B',
            type: 'integer'
          }
        ]
      }
    };
    importer.importResource(inStream, resource, function(err, out) {
      assert(stubbed.calledOnce);
      calledWith = stubbed.getCall(0).args[0];
      assert.equal(calledWith.data.resource_id, resource.id);
      assert.deepEqual(calledWith.data.fields, [
        { id: 'A', type: 'int'}, { id: 'B', type: 'int' }
      ]);
      assert.deepEqual(calledWith.data.records, [
        { A:1, B:2 }
      ]);
      done(err);
    });
  });
});
