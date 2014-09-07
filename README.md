Javascript and Node library for pushing Data Packages to CKAN.

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

