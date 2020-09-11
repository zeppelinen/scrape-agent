import random

class Luminati():

	host_port = "zproxy.lum-superproxy.io:22225"
	user = "lum-customer-qfx_capital-zone-{zone}{session}"
	zone = "static"
	password = "bcgh9syai2yg"

	def get_random_proxy(self):
		session = "-session-{rand}".format(rand = random.randint(1000000000000,100000000000000000))
		return {
			'host_port': self.host_port,
			'user': self.user.format(zone=self.zone, session=session),
			'password': self.password
		}