from threading import Thread
import socket
import ftplib
from typing import List
from urllib.parse import urlparse

import pymongo
import requests


class NosqlChecker:
    def __init__(self, url):
        self.url = url
        self.domain = urlparse(self.url).netloc
        self.ip = socket.gethostbyname(self.domain)

        self.services = []

    def check_redis(self, save=True):
        services = []
        for port in [6379, 7001, 7002, 7000, 32768, 7777, 6699, 10000, 6779]:
            try:
                socket.setdefaulttimeout(5)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.ip, port))
                s.send(bytes("INFO\r\n", 'UTF-8'))
                result = s.recv(1024).decode()
                if "redis_version" in result:
                    print(f'Found redis on port {port}')
                    services.append({
                        'product': 'redis',
                        'port': port,
                        'version': ''
                    })
                s.close()
            except Exception as e:
                print(f'redis: {e}')
                pass

        if save:
            self.services += services

        return services

    def check_mongo_db(self, save=True):
        services = []
        for port in [27017, 28017]:
            try:
                conn = pymongo.MongoClient(self.ip, 27017, socketTimeoutMS=4000)
                _ = conn.list_database_names()
                print(f'Found mongodb on port {port}')
                conn.close()
                services.append({
                    'product': 'mongodb',
                    'port': 27017,
                    'version': ''
                })
            except Exception as e:
                print(f'mongodb: {e}')
                pass

        if save:
            self.services += services

        return services

    def check_memcached(self, save=True):
        result = []

        try:
            socket.setdefaulttimeout(5)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.ip, 11211))
            s.send(bytes('stats\r\n', 'UTF-8'))
            if 'version' in s.recv(1024).decode():
                print(f'Found memcached on port {11211}')
                result.append({
                    'product': 'memcached',
                    'port': 11211,
                    'version': ''
                })
            s.close()
        except Exception as e:
            print(f'memcached: {e}')
            pass

        if save:
            self.services += result

        return result

    def check_elasticsearch(self, save=True):
        result = []

        try:
            url = f'http://{self.ip}:9200/_cat'
            r = requests.get(url, timeout=5)
            if '/_cat/master' in r.content.decode():
                print(f"Found elasticsearch on port {9200}")
                result.append({
                    'product': 'elasticsearch',
                    'port': 9200,
                    'version': ''
                })
        except Exception as e:
            print(f'elasticsearch: {e}')
            pass

        if save:
            self.services += result

        return result

    def check_zookeeper(self, save=True):
        result = []

        try:
            socket.setdefaulttimeout(5)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.ip, 2181))
            s.send(bytes('envi', 'UTF-8'))
            data = s.recv(1024).decode()
            s.close()
            if 'Environment' in data:
                print('Found zookeeper on port 2181')
                result.append({
                    'product': 'zookeeper',
                    'port': 2181,
                    'version': ''
                })
        except Exception as e:
            print(f'zookeeper: {e}')
            pass

        if save:
            self.services += result

        return result

    def check_ftp(self, save=True):
        result = []

        try:
            ftp = ftplib.FTP(self.ip)
            ftp.login('anonymous', 'Aa@12345678')
            print(f'Found ftp on port 21')
            result.append({
                'product': 'ftp',
                'port': 21,
                'version': '',
            })
        except Exception as e:
            print(f'ftp: {e}')
            pass

        if save:
            self.services += result

        return result

    def check_couchdb(self, save=True):
        result = []

        for port in [5984, 443, 5986, 80]:
            try:
                url = f'http://{self.ip}' + f':{port}' + '/_utils/'
                r = requests.get(url, timeout=5)
                if 'couchdb-logo' in r.content.decode():
                    print(f'Found couchdb on port {port}')
                    result.append({
                        'product': 'couchdb',
                        'port': port,
                        'version': ''
                    })
            except Exception as e:
                print(f'couchdb: {e}')

        if save:
            self.services += result

        return result

    def check_docker(self, save=True):
        result = []

        try:
            url = self.url + ':2375' + '/version'
            r = requests.get(url, timeout=5)
            if 'ApiVersion' in r.content.decode():
                print('Found docker api on port 2375')
                result.append({
                    'product': 'docker',
                    'port': 2375,
                    'version': ''
                })
        except Exception as e:
            print(f'docker: {e}')

        if save:
            self.services += result

        return result

    def check_hadoop(self, save=True) -> List:
        result = []

        for port in [50070, 50090, 9870, 50075, 50030]:
            try:
                url = self.url + ':50030' + '/dfshealth.html'
                r = requests.get(url, timeout=5)
                if 'hadoop.css' in r.content.decode():
                    print(f'Found hadoop on port {port}')
                    result.append({
                        'product': 'hadoop',
                        'port': port,
                        'version': ''
                    })
            except Exception as e:
                print(f'hadoop: {e}')

        if save:
            self.services += result

        return result

    def get_services(self) -> List:
        threads = [
            Thread(target=self.check_redis, name='redis_check_thread'),
            Thread(target=self.check_mongo_db, name='mongo_check_thread'),
            Thread(target=self.check_elasticsearch, name='elasticsearch_check_thread'),
            Thread(target=self.check_zookeeper, name='zookeeper_check_thread'),
            Thread(target=self.check_ftp, name='ftp_check_thread'),
            Thread(target=self.check_couchdb, name='couch_db_check_thread'),
            Thread(target=self.check_docker, name='docker_check_thread'),
            Thread(target=self.check_hadoop, name='hadoop_check_thread'),
        ]

        for thread in threads:
            thread.start()
            print(f'Thread "{thread.name}" has started')

        for thread in threads:
            thread.join()
            print(f'Thread "{thread.name}" has finished')

        return self.services
