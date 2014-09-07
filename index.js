var fs = require('fs')
  , path = require('path')
  , CKAN = require('ckan')
  , parse = require('csv-parse')
  , dpRead = require('datapackage-read')
  ;

var Importer = function(ckanInstance, apiKey) {
  this.client = new CKAN.Client(ckanInstance, apiKey);
};

module.exports = {
  Importer: Importer
};

// 1. Load the Data Package off disk
// 2. Check if Dataset already exists (i.e. same name as this Data Package)
//
// IF ALREADY EXISTS
//
// Warn
//
// IF NOT ALREADY EXISTS
//
// 1. Create Dataset object (including resources) in CKAN
// 2. Get info on created Dataset (esp resource ids)
// 3. Load data into the DataStore for each resource

// Update (Data Package already exists)
/*
config options

- overwrite - overwrite existing dataset (default true)
  - if false will fail if an existing dataset exists with the same name
- dropBeforeInsert - drop existing data in the datastore table for a resource before inserting the new data (default true)
*/
Importer.prototype.push = function(filePath, cb) {
  var that = this
    , basePath = fs.statSync(filePath).isDirectory() ? filePath : path.dirname(filePath)
    ;

  dpRead.load(filePath, function(err, dpJson) {
    if (err) {
      cb(err);
      return;
    }
    that.upsertDatasetMetadata(dpJson, function(err, createdCkanDataset) {
      if (err) {
        cb(err);
        return;
      }
      for(ii=0; ii<createdCkanDataset.resources.length; ii++) {
        dpJson.resources[ii].id = createdCkanDataset.resources[ii].id;
      }
      that.importResources(dpJson, basePath, function(err) {
        var msg = 'Data Package successfully pushed to: ' + that.client.endpoint.replace('/api', '') + '/dataset/' + dpJson.name;
        if (err) cb(err);
        else cb(err, msg);
      });
    });
  });
}

// upsert dataset metadata from a data package (JSON)
// callback result will be resulting CKAN Dataset metadata
Importer.prototype.upsertDatasetMetadata = function(dpJson, cb) {
  var that = this
    , ckanDatasetJson = _convertDataPackageToCkanDataset(dpJson);
    ;

  // check whether it already exists
  this.client.action('dataset_show', { id: dpJson.name }, function(err, out) {
    // dataset exists
    if (!err) {
      // TODO: do the update
      // client.action('dataset_update', datasetInfo, cb);
      cb(null, out.result);
    } else {
      that.client.action('dataset_create', ckanDatasetJson, function(err, out) {
        cb(err, out.result)
      });
    }
  });
};

// convert DataPackage.json metadata to CKAN style
function _convertDataPackageToCkanDataset(dpJson) {
  // TODO: more conversion of metadata structure across to CKAN
  // structure e.g. license
  var ckanDatasetJson = JSON.parse(JSON.stringify(dpJson));
  ckanDatasetJson.notes = ckanDatasetJson.readme;
  delete ckanDatasetJson.readmeHtml;
  ckanDatasetJson.url = ckanDatasetJson.homepage;
  delete ckanDatasetJson.homepage;
  ckanDatasetJson.resources.forEach(function(res) {
    res.url_type = 'datastore';
  });
  return ckanDatasetJson;
}


Importer.prototype.importResources = function(dpJson, dpBasePath, cb) {
  var that = this;

  if (!dpJson.resources || dpJson.resources.length == 0) {
    cb();
    return;
  }

  // our hack way to do parallel async calls w/o async or Q library
  var count = dpJson.resources.length
    , errors = [];
    ;
  var done = function(err) {
    if (err) {
      errors.push(err);
    }
    count--;
    if (count == 0) {
      if (errors.length >= 1) cb(errors.join('\n'));
      else cb(null);
    }
  }

  // Assume we only have Tabular Data Packages (i.e. csv data)
  // TODO: what do we do with non-CSV data ...
  for (ii=0; ii<dpJson.resources.length; ii++) {
    var res = dpJson.resources[ii];
    var dataPath = path.join(dpBasePath, res.path);
    var dataStream = fs.createReadStream(dataPath);
    that.importResource(dataStream, dpJson.resources[ii], done); 
  }
}

Importer.prototype.importResource = function(dataStream, resourceJson, cb) {
  var that = this
    , resourceId = resourceJson.id
    , fields = []
    ;

  if (resourceJson.schema && resourceJson.schema.fields) {
    fields = resourceJson.schema.fields.map(function(field) {
      var type = field.type in CKAN.jsonTableSchema2CkanTypes ? CKAN.jsonTableSchema2CkanTypes[field.type] : (field.type || 'text') ;
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
  var parser = parse({columns: true}, function(err, rows) {
    if (err) {
      cb(err);
      return;
    }
    data.records = rows;

    that.client.action('datastore_create', data, cb);
  });
  if (typeof(dataStream) == 'string') {
    parser.write(dataStream);
    parser.end();
  } else {
    dataStream.pipe(parser);
  }
};


