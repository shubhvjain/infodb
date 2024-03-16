
import couchdb

class db:
    def __init__(self, url, dbname):
        self.url = url
        self.dbname = dbname
        self.server = couchdb.Server(url)
        self.db = self.server[dbname]

    def get(self, doc_id):
        try:
            doc = self.db[doc_id]
            return doc
        except couchdb.ResourceNotFound:
            print("Document not found")
            return None

    def insert(self, data):
        try:
            doc_id, _ = self.db.save(data)
            print(f"Document inserted with id: {doc_id}")
            return doc_id
        except Exception as e:
            print(f"Error inserting document: {e}")
            return None

    def update(self, doc_id, data):
        try:
            doc = self.db[doc_id]
            data['_id'] = doc_id
            data['_rev'] = doc['_rev']
            self.db.save(data)
            print(f"Document with id {doc_id} updated")
        except couchdb.ResourceNotFound:
            print("Document not found")
        except Exception as e:
            print(f"Error updating document: {e}")

    def delete(self, doc_id):
        try:
            doc = self.db[doc_id]
            self.db.delete(doc)
            print(f"Document with id {doc_id} deleted")
        except couchdb.ResourceNotFound:
            print("Document not found")
        except Exception as e:
            print(f"Error deleting document: {e}")
