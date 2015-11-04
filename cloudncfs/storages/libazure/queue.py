from common import Storage, RequestWithMethod
from urllib2 import urlopen, URLError, Request
import base64
from xml.dom import minidom

class QueueMessage(): pass

class QueueStorage(Storage):
    def __init__(self, host, account_name, secret_key, use_path_style_uris = None):
        super(QueueStorage, self).__init__(host, account_name, secret_key, use_path_style_uris)

    def create_queue(self, name):
        req = RequestWithMethod("PUT", "%s/%s" % (self.get_base_url(), name))
        req.add_header("Content-Length", "0")
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def delete_queue(self, name):
        req = RequestWithMethod("DELETE", "%s/%s" % (self.get_base_url(), name))
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code
            
    def put_message(self, queue_name, payload):
        data = "<QueueMessage><MessageText>%s</MessageText></QueueMessage>" % base64.encodestring(payload)
        req = RequestWithMethod("POST", "%s/%s/messages" % (self.get_base_url(), queue_name), data=data)
        req.add_header("Content-Type", "application/xml")
        req.add_header("Content-Length", len(data))
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def get_message(self, queue_name):
        req = Request("%s/%s/messages" % (self.get_base_url(), queue_name))
        self._credentials.sign_request(req)
        response = urlopen(req)
        dom = minidom.parseString(response.read())
        messages = dom.getElementsByTagName("QueueMessage")
        result = None
        if len(messages) == 1:
            message = messages[0]
            result = QueueMessage()
            result.id = message.getElementsByTagName("MessageId")[0].firstChild.data
            result.pop_receipt = message.getElementsByTagName("PopReceipt")[0].firstChild.data
            result.text = base64.decodestring(message.getElementsByTagName("MessageText")[0].firstChild.data)
        return result

    def delete_message(self, queue_name, message):
        id = message.id
        pop_receipt = message.pop_receipt
        req = RequestWithMethod("DELETE", "%s/%s/messages/%s?popreceipt=%s" % (self.get_base_url(), queue_name, id, pop_receipt))
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code
