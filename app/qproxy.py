from qmysql import *

class QProxy():

	def __init__(self):
		self.db = QMySql()
		self.db.connect()

	def _get_random_proxy(self, website=None):
		sql = "SELECT proxy_list.host, proxy_list.port FROM proxy_website\
			LEFT JOIN proxy_result ON proxy_result.website_id = proxy_website.id\
			LEFT JOIN proxy_list ON proxy_result.proxy_id = proxy_list.id\
			WHERE {condition} \
			proxy_result.status = 'OK'\
			ORDER BY -LOG(1.0 - RAND()) / proxy_list.weight\
			LIMIT 1".format(condition="proxy_website.website = %(website)s AND" if website is not None else "")
		return self.db.fetchone(
			sql,
			{'website': website}
		)

	def get_random_proxy(self, website=None):
		proxy = self._get_random_proxy(website)
		#fetch for at least something from DB which is not blocked for someone else
		if proxy is None:
			proxy = self._get_random_proxy()
		if proxy is not None:
			proxy_host_port = "{host}:{port}".format(host=proxy[0], port=proxy[1])
			return proxy_host_port
		else:
			return None
