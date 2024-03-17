
import couchdb
from . import util as ut
from jsonschema import validate


class db:
    def __init__(self, url, db_name, schema_name, schema_json_string):
        self.url = url
        self.dbName = db_name
        self.schemaName = schema_name
        self.schema = ut.process_schema(schema_json_string)
        self.server = couchdb.Server(url)
        self.db = self.server[db_name]

    def get_single_doc(self,search_criteria):
        try:
            doc_obj = {"schema": { "$eq": self.schemaName }}
            for k in self.schema["primary_key"]:
                doc_obj["data."+k] = { "$eq": search_criteria[k] }
            doc_check = {"selector": doc_obj}
            docs_found = self.db.find(doc_check)
            docs_test = []
            for r in docs_found:
                docs_test.append(dict(r))
            if len(docs_test) > 0:
                the_doc = docs_test[0]
                return the_doc
            else :
                return None 
        except Exception as e:
            raise e    
    
    def get(self, doc_id):
        try:
            doc = self.db[doc_id]
            if doc["schema"] != self.schemaName:
                raise Exception("Not found")
            return dict(doc)
        except couchdb.ResourceNotFound:
            return None

    def get_using_key(self,criteria):
        try:
            doc_search = self.get_single_doc(criteria)
            if doc_search :
                if doc_search["schema"] != self.schemaName:
                    raise Exception("Not found")
                return doc_search
        except couchdb.ResourceNotFound:
            return None        

    def check_if_doc_exists(self,data):
        try:
            doc_search = self.get_single_doc(data)
            if doc_search is not None :
                raise Exception("Document already exists. Insert failed")
        except Exception as e:
            raise e
    
    def insert(self, data):
        try:
            validate(data,self.schema)
            self.check_if_doc_exists(data)
            new_doc = ut.generate_blank_doc(self.schemaName,self.schema)
            new_doc["data"] = data
            self.db.save(new_doc)
            return new_doc
        except Exception as e:
            print(f"Error inserting document: {e}")
            return None

    def update(self, doc_id, updates={}):
        try:
            # first check if the schema matches
            data = self.db[doc_id]
            if data["schema"] != self.schemaName:
                raise Exception("Not found")
            data['_id'] = doc_id
            # check if update fields are allowed to be updated
            if ut.are_all_keys_allowed(updates,self.schema["update_allowed"]):
                data["data"]  = ut.update_dict(data["data"],updates)
                data["meta"]["updated_on"] = ut.get_current_unix_timestamp()
                self.db.save(data)
            else:
                raise Exception("Updated not allowed.")
            print(f"Document with id {doc_id} updated")
        except couchdb.ResourceNotFound:
            print("Document not found")
        except Exception as e:
            print(f"Error updating document: {e}")

    def delete(self, doc_id):
        try:
            # first check if schema matches
            doc = self.db[doc_id]
            if doc["schema"] != self.schemaName:
                raise Exception("Not found")
            self.db.delete(doc)
        except couchdb.ResourceNotFound:
            print("Document not found")
        except Exception as e:
            print(f"Error deleting document: {e}")
    
    def search(self,criteria):
        try:
            criteria["schema"] = { "$eq": self.schemaName }
            docs_found = self.db.find({"selector":criteria})
            results = []
            for r in docs_found:
                results.append(dict(r))
            return {"records":results,"count":len(results)}
        except Exception as e:
            raise e    
