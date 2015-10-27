from common import Storage, RequestWithMethod, parse_edm_int32,\
    parse_edm_boolean, parse_edm_double, parse_edm_datetime
import time
from urllib2 import urlopen, URLError, Request
from xml.dom import minidom

class TableEntity(object): pass

class Table(object):
    def __init__(self, url, name):
        self.url = url
        self.name = name

class TableStorage(Storage):
    '''Due to local development storage not supporting SharedKey authentication, this class
       will only work against cloud storage.'''
    def __init__(self, host, account_name, secret_key, use_path_style_uris = None):
        super(TableStorage, self).__init__(host, account_name, secret_key, use_path_style_uris)

    def create_table(self, name):
        data = """<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<entry xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
  <title />
  <updated>%s</updated>
  <author>
    <name />
  </author>
  <id />
  <content type="application/xml">
    <m:properties>
      <d:TableName>%s</d:TableName>
    </m:properties>
  </content>
</entry>""" % (time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()), name)
        req = RequestWithMethod("POST", "%s/Tables" % self.get_base_url(), data=data)
        req.add_header("Content-Length", "%d" % len(data))
        req.add_header("Content-Type", "application/atom+xml")
        self._credentials.sign_table_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def delete_table(self, name):
        req = RequestWithMethod("DELETE", "%s/Tables('%s')" % (self.get_base_url(), name))
        self._credentials.sign_table_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code
        
    def list_tables(self):
        req = Request("%s/Tables" % self.get_base_url())
        self._credentials.sign_table_request(req)
        response = urlopen(req)

        dom = minidom.parseString(response.read())
        
        entries = dom.getElementsByTagName("entry")
        for entry in entries:
            table_url = entry.getElementsByTagName("id")[0].firstChild.data
            table_name = entry.getElementsByTagName("content")[0].getElementsByTagName("m:properties")[0].getElementsByTagName("d:TableName")[0].firstChild.data
            yield Table(table_url, table_name)
        dom.unlink()

    def get_entity(self, table_name, partition_key, row_key):
        dom = minidom.parseString(urlopen(self._credentials.sign_table_request(Request("%s/%s(PartitionKey='%s',RowKey='%s')" % (self.get_base_url(), table_name, partition_key, row_key)))).read())
        entity = self._parse_entity(dom.getElementsByTagName("entry")[0])
        dom.unlink()
        return entity

    def _parse_entity(self, entry):
        entity = TableEntity()
        for property in (p for p in entry.getElementsByTagName("m:properties")[0].childNodes if p.nodeType == minidom.Node.ELEMENT_NODE):
            key = property.tagName[2:]
            if property.hasAttribute('m:type'):
                t = property.getAttribute('m:type')
                if t.lower() == 'edm.datetime': value = parse_edm_datetime(property.firstChild.data)
                elif t.lower() == 'edm.int32': value = parse_edm_int32(property.firstChild.data)
                elif t.lower() == 'edm.boolean': value = parse_edm_boolean(property.firstChild.data)
                elif t.lower() == 'edm.double': value = parse_edm_double(property.firstChild.data)
                else: raise Exception(t.lower())
            else: value = property.firstChild is not None and property.firstChild.data or None
            setattr(entity, key, value)
        return entity

    def get_all(self, table_name):
        dom = minidom.parseString(urlopen(self._credentials.sign_table_request(Request("%s/%s" % (self.get_base_url(), table_name)))).read())
        entries = dom.getElementsByTagName("entry")
        entities = []
        for entry in entries:
            entities.append(self._parse_entity(entry))
        dom.unlink()
        return entities
