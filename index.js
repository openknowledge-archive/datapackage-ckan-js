var fs = require('fs')
  , path = require('path')
  , CKAN = require('ckan')
  , parse = require('csv-parse')
  , dpRead = require('datapackage-read')
  ;

var Pusher = function(ckanInstance, apiKey) {
  this.client = new CKAN.Client(ckanInstance, apiKey);
};

module.exports = {
  Pusher: Pusher
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
//

/*
config options

- owner_org - owner organization name for CKAN dataset

Ideas
- overwrite - overwrite existing dataset (default true)
  - if false will fail if an existing dataset exists with the same name
- dropBeforeInsert - drop existing data in the datastore table for a resource before inserting the new data (default true)
*/
Pusher.prototype.push = function(filePath, config, cb) {
  var that = this
    , basePath = fs.statSync(filePath).isDirectory() ? filePath : path.dirname(filePath)
    ;
  if (arguments.length < 3) {
    cb = config;
    config = {};
  }
  dpRead.load(filePath, function(err, dpJson) {
    if (err) {
      cb(err);
      return;
    }
    console.log('Loaded Data Package');

    // HACK - insert owner_org onto dpJson so it ends up on CKAN dataset metadata
    // need to think how to do this better
    // probably should be in _convertDataPackageToCkanDataset
    if (config.owner_org) {
      dpJson.owner_org = config.owner_org;
    }

    that.upsertDatasetMetadata(dpJson, function(err, createdCkanDataset) {
      console.log('Created/Updated CKAN Dataset with Data Package Metadata');
      if (err) {
        cb(err);
        return;
      }
      for(ii=0; ii<createdCkanDataset.resources.length; ii++) {
        dpJson.resources[ii].id = createdCkanDataset.resources[ii].id;
      }
      that.pushResources(dpJson, basePath, function(err) {
        var msg = 'Data Package successfully pushed to: ' + that.client.endpoint.replace('/api', '') + '/dataset/' + dpJson.name;
        if (err) cb(err);
        else cb(err, msg);
      });
    });
  });
}

// upsert dataset metadata from a data package (JSON)
// callback result will be resulting CKAN Dataset metadata
Pusher.prototype.upsertDatasetMetadata = function(dpJson, cb) {
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


Pusher.prototype.pushResources = function(dpJson, dpBasePath, cb) {
  var that = this;

  if (!dpJson.resources || dpJson.resources.length == 0) {
    cb();
    return;
  }

  // our hack way to do serial async calls w/o async or Q library
  var count = -1
    , errors = []
    ;
  var done = function(err) {
    if (err) {
      errors.push(err);
    }
    count++;
    if (count == dpJson.resources.length) {
      if (errors.length >= 1) cb(errors.join('\n'));
      else cb(null);
    } else {
      var res = dpJson.resources[count];
      _doImport(res, done);
    }
  }

  // Assume we only have Tabular Data Packages (i.e. csv data)
  // TODO: what do we do with non-CSV data ...
  function _doImport(resource, cb) {
    var dataPath = path.join(dpBasePath, resource.path);
    var dataStream = fs.createReadStream(dataPath);
    that.pushResourceData(dataStream, resource, cb); 
  }

  // start off looping through resources
  done();
}

Pusher.prototype.pushResourceData = function(dataStream, resourceJson, cb) {
  var that = this
    , resourceId = resourceJson.id
    , fields = []
    ;

  console.log('Pushing data to CKAN DataStore for resource: ' + resourceJson.name);

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
  // drop any existing data first ...
  // we ignore errors from datastore_delete as likely just 404 (i.e. no datastore table yet)
  // TODO: check that errors really are 404 (and not something else)
  that.client.action('datastore_delete', {resource_id: resourceId}, function(err) {
    that.client.action('datastore_create', data, function(err) {
      if (err) {
        cb(err);
        return;
      }
      that._loadDataToDataStore(dataStream, resourceId, cb);
    });
  });
};

// load rows of data one chunk at a time
Pusher.prototype._loadDataToDataStore = function(dataStream, resourceId, cb) {
  var that = this
    , offset = 0
    , chunkSize = 10000
    , rows = null
    ;

  // assume a header row on the CSV file
  var parser = parse({columns: true}, function(err, _rows) {
    rows = _rows;
    loadData(err);
  });

  function loadData(err) {
    if (err) {
      console.error(err);
      cb(err);
      return;
    }
    // we are finished
    if (offset > rows.length) {
      cb();
      return;
    }

    console.log('Done rows: ' + offset);

    var data = {
      resource_id: resourceId,
      method: 'insert',
      records: rows.slice(offset, offset+chunkSize)
    };
    offset += chunkSize;
    that.client.action('datastore_upsert', data, loadData);
  }

  // now start it running
  if (typeof(dataStream) == 'string') {
    parser.write(dataStream);
    parser.end();
  } else {
    dataStream.pipe(parser);
  }
}

