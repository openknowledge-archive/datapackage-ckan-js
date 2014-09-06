var CKAN = require('ckan')
  , csv = require('csv')
  , parse = require('csv-parse')
  , dpRead = require('datapackage-read')
  ;

var Importer = function(ckanInstance, apiKey) {
  this.client = new CKAN.Client(ckanInstance, apiKey);
};

module.exports = {
  Importer: Importer
};

Importer.prototype.importDataPackage = function(dataStream, resourceJson, cb) {
}

Importer.prototype.importResource = function(dataStream, resourceJson, cb) {
  var self = this;
  // TODO: get this from the CKAN instance (?)
  var resourceId = resourceJson.id;
  var fields = [];
  if (resourceJson.schema && resourceJson.schema.fields) {
    fields = resourceJson.schema.fields.map(function(field) {
      var type = field.type in CKAN.jsonTableSchema2CkanTypes ? CKAN.jsonTableSchema2CkanTypes[field.type] : field.type;
      return {
        id: field.name,
        type: type
      };
    });
  }
  var data ={
    fields: fields,
    resource_id: resourceId
  };
  if (resourceJson.primaryKey) {
    data.primary_key = resourceJson.primaryKey
  }
  // assume a header row on the CSV file
  parse(dataStream, {columns: true}, function(err, rows) {
    data.records = rows;
    self.client.action('datastore_create', data, cb);
  });
};

