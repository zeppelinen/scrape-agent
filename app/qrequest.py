import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.auth import HTTPProxyAuth

class Qrequest():

	proxy = None
	timeout = 3 #sec
	session = None

	def __init__(self, args={}):
		if 'proxy' in args:
			self.set_proxy(args['proxy'])
		if 'timeout' in args:
			self.timeout = args['timeout']
		self.set_http_session()

	def set_http_session(self, retries=2, backoff_factor=0.2, status_forcelist=(500, 502, 504)):
		session = requests.Session()
		retry = Retry(
			total=retries,
			read=retries,
			connect=retries,
			backoff_factor=backoff_factor,
			status_forcelist=status_forcelist,
		)
		adapter = HTTPAdapter(max_retries=retry)
		session.mount('http://', adapter)
		session.mount('https://', adapter)
		self.session = session

	def set_proxy(self, proxy):
		self.proxy = proxy

	def set_headers(self, headers):
		self.session.headers.update(headers)

	def http_get(self, url, attempt=1):
		response = {}
		proxy = {}
		#print('GETTING {url}'.format(url=url))
		if self.proxy is not None:
			proxy_url = self.proxy['host_port']
			if 'user' in self.proxy and self.proxy['user'] is not None:
				proxy_url = '{user}:{password}@{host_port}'.format(user=self.proxy['user'], password=self.proxy['password'], host_port=self.proxy['host_port'])
			proxies = dict(
				http="http://{proxy_url}".format(proxy_url=proxy_url), 
				https="https://{proxy_url}".format(proxy_url=proxy_url)
			)
			self.session.proxies = proxies
		try:
			r = self.session.get(url, timeout=self.timeout)
			#print(r.headers)
			response = {
				'result': True,
				'http_code': r.status_code,
				'text': r.text,
				#'json': r.json(),
				'attempt': attempt,
			}
			if r.status_code == 200:
				try:
					response['json'] = r.json()
				except ValueError:
					pass

		except requests.HTTPError as e:
			# Maybe set up for a retry, or continue in a retry loop
			response = {
				'result': False,
				'error': 'http {e}'.format(e=e),
				'http_code': e,
				'attempt': attempt,
			}
		except requests.exceptions.RequestException as e:
			# catastrophic error. bail.
			response = {
				'result': False,
				'error': e,
				'attempt': attempt,
			}

		return response
