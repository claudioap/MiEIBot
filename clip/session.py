import clip
import os
import requests
from http.cookiejar import LWPCookieJar
import logging

log = logging.getLogger(__name__)
__active_sessions__ = []

http_headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:1.0) Gecko/20100101 MiEIBot'}
login_params = {'identificador': 'NOPE', 'senha': 'BIG NOOOPE'}  # TODO move somewhere else to prevent accidents


class Session:

    def __init__(self, cookies=os.getcwd() + '/cookies'):
        log.info('Creating clip session (Cookie file:{})'.format(cookies))
        self.__cookie_file__ = cookies
        self.authenticated = False
        self.__requests_session__ = requests.Session()
        self.__requests_session__.cookies = LWPCookieJar(cookies)
        for session in __active_sessions__:
            if session.__cookie_file__ == self.__cookie_file__:
                raise Exception("Attempted to share a cookie file")

        if not os.path.exists(cookies):
            self.save()
            log.info('Created empty cookie file')
        __active_sessions__.append(self)
        self.authenticate()

    def save(self):
        self.__requests_session__.cookies.save(ignore_discard=True)

    def authenticate(self):
        request = self.__requests_session__.post(clip.base_url, headers=http_headers, data=login_params)
        log.info('Successfully authenticated')
        # TODO verify if succeeded
        self.save()

    def get(self, url):
        log.info('Fetching:' + url)
        return self.__requests_session__.get(url, headers=http_headers)


session = Session()
