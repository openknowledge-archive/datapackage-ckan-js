Javascript and Node library for connecting Data Packages to CKAN.

Specifically the library supports:

* Pushing / importing a complete Data Package including data files into CKAN
  (data gets stored in the CKAN DataStore).
* Pushing individual resources with their data to CKAN

If you want to have a nice command line interface we recommend using the `ckan`
command in [`dpm` (Data Packagfe Manager)][dpm].

[dpm]: http://github.com/okfn/dpm

# Installation

[![NPM](https://nodei.co/npm/datapackage-ckan.png)](https://nodei.co/npm/datapackage-ckan/)

```
npm install datapackage-ckan
```

# Usage

```
var dp2ckan = require('datapackage-ckan');

var pusher = dp2ckan.Pusher(ckanInstanceUrl, ckanApiKey);
pusher.push(pathToDataPackage);
```

# License

(c) 2014 Rufus Pollock

Licensed under the MIT License.

