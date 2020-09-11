# Scrapers agents

Multiple agents scraping Web Sockets, REST APIs, and public sources

## Main Prerequisites

**Python 3.6.x**

pip package manager (is bundled since Python 3.4)
packages:
 - websocket
 - redis
 - google-cloud-logging
 - google-cloud-pubsub
(full requirements are at requirements.txt)

**Redis server**

### Installing

Install Python 3.6.x

```
sudo apt-get update
sudo apt-get install python3
```

Install extra packages

```
python3 -m pip install websockets
python3 -m pip install redis
python3 -m pip install google-cloud-logging
python3 -m pip install google-cloud-pubsub
```

Install and configure Redis

```
sudo apt update
sudo apt install redis-server
```

configure Redis

```
sudo vi /etc/redis/redis.conf
```
for Ubuntu 18.04 set the following config:
```
...
supervised systemd
...
bind 127.0.0.1 ::1
#use 0.0.0.0 to open Redis to the remote connections
...
requirepass YOURPASSWORD
```
restart Redis
```
sudo systemctl restart redis
```

