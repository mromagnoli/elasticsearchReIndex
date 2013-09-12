"""
__author__ = "Marcelo Romagnoli"
__copyright__ = "Copyright 2013"
__credits__ = ["Zachary Tong | http://euphonious-intuition.com/2012/08/reindexing-your-elasticsearch-data-with-scanscroll"]
__version__ = "0.1"
__maintainer__ = "Marcelo Romagnoli"
__email__ = "marce.romagnoli@gmail.com"
"""
import pycurl
import cStringIO
import json
import sys
import getopt


def makeCurl(URI, method, data = ''):
	"""Used as curl wrapper.

		Generate curl calls needed for ES.
		Args:
			URI: Destination of the curl request.
			method: Method of HTTP request.
			data: Optional data to send.
		Returns:
			Response from destination.
	"""

	c = pycurl.Curl()
	c.setopt(c.URL, str(URI))
	c.setopt(c.CUSTOMREQUEST, method)

	if data != '':
		c.setopt(c.POSTFIELDS, str(data))

	buf = cStringIO.StringIO()
	c.setopt(c.WRITEFUNCTION, buf.write)

	try:
		c.perform()
	except Exception, e:
		print(e)
		sys.exit()

	c.close()

	return buf.getvalue()

def nextScroll(scrollId = '', host = 'localhost:9200/'):
	"""Retrieve next batch of results based on scroll id.
		Args:
			scrollId: Id given by ES for next scroll.
			host: Domain of ES.
		Returns:
			Data dictionary from ES.
	"""

	if scrollId == '':
		print('Missing scroll id.')
		sys.exit(2)

	uri = host + '/_search/scroll?scroll=10m'
	response = makeCurl(uri, 'GET', scrollId)
	data =  json.loads(response)
	return data

def manageOpts(argv):
	"""Manage command shell options.
		Args:
			argv: List of arguments from shell.
	"""
	try:
		opts, args = getopt.getopt(argv, 'ho:n:t:s:', ['help', 'index-old', 'index-new', 'type', 'host'])
	except getopt.GetoptError as err:
		usage();
		sys.exit(2)

	global INDEX_OLD, INDEX_NEW, INDEX_TYPE, HOST
	INDEX_OLD = ''
	INDEX_NEW = ''
	INDEX_TYPE = ''
	HOST = 'http://192.168.222.93:9200/'

	for opt, arg in opts:
		if opt in ('-h', '--help'):
			usage()
			sys.exit()
		elif opt in ('-o', '--index-old'):
			INDEX_OLD = arg
		elif opt in ('-n', '--index-new'):
			INDEX_NEW = arg
		elif opt in ('-t', '--type'):
			INDEX_TYPE = arg
		elif opt in ('-s', '--host'):
			HOST = arg

	if INDEX_OLD == '' or INDEX_NEW == '' or INDEX_TYPE == '':
		usage()
		sys.exit()

def usage():
	"""Usage of script."""
	message = """
	USAGE: reindex_es.py -o <old-index> -n <new-index> -t <index-type> [-s <host>]

	-h, --help: Display this help.
	-o, --old-index: Index name from where data is pulled.
	-n, --new-index: Index name where data is pushed, if not exists it is created.
	-t, --type: Index type for destination index.
	-s, --host: Host where ES lives. Default 'http://192.168.222.93:9200/'
	"""
	print(message)

def checkErrors(data):
	"""Check if 'error' key exists in data.
		Args:
			data: Response provided by ES where we look for 'error' key.
	"""
	if 'error' in data:
		print('ERROR: {}').format(data['error'])
		return True
	return False


# MAIN
if __name__ == "__main__":
	manageOpts(sys.argv[1:])

	# Retrieve all documents
	query = json.dumps({
		'query' : {
			'match_all' : {}
		}
	})


	# 'scroll' param indicates that session should be valid for 10 minutes, then expires.
	# 'size' param indicates how many results should be returned in each scroll.
	uri = HOST + INDEX_OLD + '/' + INDEX_TYPE + '/_search?search_type=scan&scroll=10m&size=100'
	response = makeCurl(uri, 'GET', query)
	data = json.loads(response)

	if checkErrors(data):
		sys.exit(2)

	# Scroll session id, used to request the next batch of data
	scrollId = data['_scroll_id'];

	# Get the total docs to retrieve
	totalHits = data['hits']['total']

	# Now query ES and provide this id to start retrieving the data
	data =  nextScroll(scrollId, HOST)

	counter = 0;
	errors = 0
	while len(data['hits']['hits']) > 0:
		for doc in data['hits']['hits']:
			put = json.dumps(doc['_source'])

			uri = HOST + INDEX_NEW + '/' + INDEX_TYPE + '/' + doc['_id']

			print('--------------')
			# Three intents or exit
			while not errors > 2:
				response = json.loads(makeCurl(uri, 'PUT', put))
				if checkErrors(data):
					errors += 1
				else:
					print('Successfully added "_id": {}').format(doc['_id'])
					break

			if errors >= 2:
				print('Three times failed adding "_id": {}.').format(doc['_id'])
				print('Leaving process...')
				sys.exit(2)

			counter += 1

			percentage = (float(counter) / float(totalHits)) * 100
			print('DONE: {} %').format(percentage)
			print('{} / {}').format(counter, totalHits)

		# New scroll id to continue
		scrollId = data['_scroll_id']

		# Query ES and provide this id to start retrieving the data
		data =  nextScroll(scrollId, HOST)