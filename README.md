elasticsearchReIndex
====================

Simple Python script for reindex elasticsearch index in a new one.


This script let you reindex an existing ElasticSearch index into another, in other words 'copy' all the documents from an index in a new one, where, for example, you have created new mappings or settings.

Usage
--------

``` bash
In command line:
      python reindex_es.py -o <old-index> -n <new-index> -t <index-type> [-s <host>]

	-h, --help: Display help.
	-o, --old-index: Index name from where data is pulled.
	-n, --new-index: Index name where data is pushed, if not exists it is created.
	-t, --type: Index type for destination index.
	-s, --host: Host where ES lives. Default 'http://localhost:9200/'
```
