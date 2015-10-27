from common import Storage, PREFIX_PROPERTIES, RequestWithMethod, TIME_FORMAT
from urllib2 import urlopen, URLError, Request
from xml.dom import minidom
import time

class BlobStorage(Storage):
    def __init__(self, host, account_name, secret_key , use_path_style_uris = None):
        super(BlobStorage, self).__init__(host, account_name, secret_key, use_path_style_uris)

    def create_container(self, container_name, is_public = False):
        req = RequestWithMethod("PUT", "%s/%s" % (self.get_base_url(), container_name))
        req.add_header("Content-Length", "0")
        if is_public: req.add_header(PREFIX_PROPERTIES + "publicaccess", "true")
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def delete_container(self, container_name):
        req = RequestWithMethod("DELETE", "%s/%s" % (self.get_base_url(), container_name))
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def list_containers(self):
        req = Request("%s/?comp=list" % self.get_base_url())
        self._credentials.sign_request(req)
        dom = minidom.parseString(urlopen(req).read())
        containers = dom.getElementsByTagName("Container")
        for container in containers:
            container_name = container.getElementsByTagName("Name")[0].firstChild.data
            etag = container.getElementsByTagName("Etag")[0].firstChild.data
            last_modified = time.strptime(container.getElementsByTagName("LastModified")[0].firstChild.data, TIME_FORMAT)
            yield (container_name, etag, last_modified)

        dom.unlink() #Docs say to do this to force GC. Ugh.

    def list_blobs(self, container_name):
        req = Request("%s/%s?comp=list" % (self.get_base_url(), container_name))
        self._credentials.sign_request(req)
        dom = minidom.parseString(urlopen(req).read())
        blobs = dom.getElementsByTagName("Blob")
        res = []
        for blob in blobs:
            res.append(blob.getElementsByTagName("Name")[0].firstChild.data)
        dom.unlink()
        return res


    def put_blob(self, container_name, blob_name, data, content_type = None, metadata = {}):
        req = RequestWithMethod("PUT", "%s/%s/%s" % (self.get_base_url(), container_name, blob_name), data=data)
        req.add_header("Content-Length", "%d" % len(data))
        if content_type is not None: req.add_header("Content-Type", content_type)
        for name, value in metadata.iteritems():
            req.add_header("x-ms-meta-" + name, value)
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def set_metadata(self, container_name, blob_name, metadata):
        req = RequestWithMethod("PUT", "%s/%s/%s?comp=metadata" % (self.get_base_url(), container_name, blob_name))
        for name, value in metadata.iteritems():
            req.add_header("x-ms-meta-" + name, value)
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def get_blob(self, container_name, blob_name, offset = None, size = None):
        req = Request("%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
        if offset is not None and size is not None:
            req.add_header("Range", ("bytes=%d-%d" % (offset, offset + size - 1)))
        self._credentials.sign_request(req)
        return urlopen(req).read()

    def get_metadata(self, container_name, blob_name):
        req = Request("%s/%s/%s?comp" % (self.get_base_url(), container_name, blob_name))
        self._credentials.sign_request(req)
        metadata = {}
        for name, value in urlopen(req).info().items():
            if name.startswith("x-ms-meta-"):
                metadata[name[10:]] = value
        return metadata

    def delete_blob(self, container_name, blob_name):
        req = RequestWithMethod("DELETE", "%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
        self._credentials.sign_request(req)
        return urlopen(req).read()
