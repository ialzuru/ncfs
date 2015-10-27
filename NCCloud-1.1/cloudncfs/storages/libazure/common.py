from urllib2 import Request
import base64 
import re
import time
import hmac
import hashlib
from datetime import datetime, timedelta

PREFIX_PROPERTIES = "x-ms-prop-"
PREFIX_METADATA = "x-ms-meta-"
PREFIX_STORAGE_HEADER = "x-ms-"

NEW_LINE = "\x0A"

TIME_FORMAT ="%a, %d %b %Y %H:%M:%S %Z"

from urlparse import urlsplit

class RequestWithMethod(Request):
    '''Subclass urllib2.Request to add the capability of using methods other than GET and POST.
       Thanks to http://benjamin.smedbergs.us/blog/2008-10-21/putting-and-deleteing-in-python-urllib2/'''
    def __init__(self, method, *args, **kwargs):
        self._method = method
        Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method

class SharedKeyCredentials(object):
    def __init__(self, account_name, account_key, use_path_style_uris = None):
        self._account = account_name
        self._key = base64.decodestring(account_key)

    def _sign_request_impl(self, request, for_tables = False,  use_path_style_uris = None):
        (scheme, host, path, query, fragment) = urlsplit(request.get_full_url())
        if use_path_style_uris:
            path = path[path.index('/'):]

        canonicalized_resource = "/" + self._account + path
        match = re.search(r'comp=[^&]*', query)
        if match is not None:
            canonicalized_resource += "?" + match.group(0)
            
        if use_path_style_uris is None:
            use_path_style_uris = re.match('^[\d.:]+$', host) is not None

        request.add_header(PREFIX_STORAGE_HEADER + 'date', time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())) #RFC 1123
        canonicalized_headers = NEW_LINE.join(('%s:%s' % (k.lower(), request.get_header(k).strip()) for k in sorted(request.headers.keys(), lambda x,y: cmp(x.lower(), y.lower())) if k.lower().startswith(PREFIX_STORAGE_HEADER)))

        string_to_sign = request.get_method().upper() + NEW_LINE # verb
        string_to_sign += NEW_LINE                               # MD5 not required
        if request.get_header('Content-type') is not None:       # Content-Type
            string_to_sign += request.get_header('Content-type')
        string_to_sign += NEW_LINE
        if for_tables: string_to_sign += request.get_header(PREFIX_STORAGE_HEADER.capitalize() + 'date') + NEW_LINE
        else: string_to_sign += NEW_LINE                         # Date
        if not for_tables:
            string_to_sign += canonicalized_headers + NEW_LINE   # Canonicalized headers
        string_to_sign += canonicalized_resource                 # Canonicalized resource
        
        request.add_header('Authorization', 'SharedKey ' + self._account + ':' + base64.encodestring(hmac.new(self._key, unicode(string_to_sign).encode("utf-8"), hashlib.sha256).digest()).strip())
        return request

    def sign_request(self, request, use_path_style_uris = None):
        return self._sign_request_impl(request, use_path_style_uris)

    def sign_table_request(self, request, use_path_style_uris = None):
        return self._sign_request_impl(request, for_tables = True, use_path_style_uris = use_path_style_uris)

class Storage(object):
    def __init__(self, host, account_name, secret_key, use_path_style_uris):
        self._host = host
        self._account = account_name
        self._key = secret_key
        if use_path_style_uris is None:
            use_path_style_uris = re.match(r'^[^:]*[\d:]+$', self._host)
        self._use_path_style_uris = use_path_style_uris
        self._credentials = SharedKeyCredentials(self._account, self._key)
        
    def get_base_url(self):
        if self._use_path_style_uris:
            return "http://%s/%s" % (self._host, self._account)
        else:
            return "http://%s.%s" % (self._account, self._host)
        
def parse_edm_datetime(input):
    d = datetime.strptime(input[:input.find('.')], "%Y-%m-%dT%H:%M:%S")
    if input.find('.') != -1:
        d += timedelta(0, 0, int(round(float(input[input.index('.'):-1])*1000000)))
    return d        

def parse_edm_int32(input):
    return int(input)

def parse_edm_double(input):
    return float(input)

def parse_edm_boolean(input):
    return input.lower() == "true"
