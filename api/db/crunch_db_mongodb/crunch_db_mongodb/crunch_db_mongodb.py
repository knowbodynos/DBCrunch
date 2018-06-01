import json
from pprint import pprint
from bson import BSON
from errno import ENOENT
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x
from pymongo.errors import DocumentTooLarge, BulkWriteError
from pymongojoin import JoinedClient, JoinedCollections, WriteConcern, UpdateOne, InsertOne, ASCENDING, DESCENDING

def merge_dicts(*dicts):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    result = dicts[0].copy()
    for d in dicts[1:]:
        result.update(d)
    return result

class DatabaseReader(object):
    def __init__(self, db_info):
        # Initialize private variables

        db_host = str(db_info.host) + ":" + str(db_info.port) + "/" + db_info.name
        if db_info.username and db_info.password:
            db_host = db_info.username + ":" + db_info.password + "@" + db_host
        db_uri = "mongodb://" + db_host
        db_client = JoinedClient(db_uri)
        db_database = db_client[db_info.name]
        db_collections = JoinedCollections()
        for collection_name in db_info.collections:
            db_collections.join(db_database[collection_name])
        find_kwargs = {
                        "no_cursor_timeout": True,
                        "allow_partial_results": True
                      }

        self.__db_info = db_info
        self.__db_client = db_client
        self.__db_collections = db_collections
        self.__find_kwargs = find_kwargs

        if self.__db_info.hint:
            self.__db_info.hint = list(self.__db_info.hint.items())

        if self.__db_info.sort:
            self.__db_info.sort = list(self.__db_info.sort.items())

        # Initialize public variables
        
        self.batch = []

        self.restart(**find_kwargs)

    def restart(self, **kwargs):
        self.__find_kwargs.update(kwargs)
        self.__db_cursor = self.__db_collections.find(self.__db_info.query, self.__db_info.projection, **self.__find_kwargs)
        self.__db_cursor = self.__db_cursor.hint(self.__db_info.hint)
        self.__db_cursor = self.__db_cursor.skip(self.__db_info.skip)
        self.__db_cursor = self.__db_cursor.limit(self.__db_info.limit)
        if self.__db_info.sort:
            self.__db_cursor = self.__db_cursor.sort(self.__db_info.sort)

        self.done = False

    def read(self, n):
        doc_count = 0
        if len(self.batch) < n:
            self.done = True
            for doc in self.__db_cursor:
                self.done = False
                self.batch.append(doc)
                doc_count += 1
                if len(self.batch) == n:
                    break
        return doc_count

    def close(self):
        self.__db_client.close()

class DatabaseWriter(Queue):
    def __init__(self, db_info, out_local = False, out_db = False, stats_local = False, stats_db = False, ordered = False):
        # Initialize Queue

        Queue.__init__(self)

        # Initialize private variables

        db_host = str(db_info.host) + ":" + str(db_info.port) + "/" + db_info.name
        if db_info.username and db_info.password:
            db_host = db_info.username + ":" + db_info.password + "@" + db_host
        db_uri = "mongodb://" + db_host
        self.__db_client = JoinedClient(db_uri)
        self.__db_database = self.__db_client[db_info.name]
        self.__write_concern = db_info.writeconcern
        self.__fsync = db_info.fsync
        self.__out_local = out_local
        self.__out_db = out_db
        self.__stats_local = stats_local
        self.__stats_db = stats_db
        self.__ordered = ordered
        self.__batch = {}
        self.__log_lines = []

        # Initialize public variables

        self.indexes = self.__db_database[db_info.basecollection].get_indexes()
        self.collections = {}

        self.count = 0
        self.bson_size = 0

    def new_request(self, action, collection, index_doc, doc):
        if collection not in self.__batch:
            self.__batch[collection] = {}

        if action not in self.__batch[collection]:
            self.__batch[collection][action] = []

        self.__batch[collection][action].append((index_doc, doc))

        if action != "none":
            if action == "unset":
                self.bson_size -= len(BSON.encode(doc))
            else:
                self.bson_size += len(BSON.encode(doc))

        if action in ["none", "stat", "mark"]:
            bkp_ext = collection + ".set"
        else:
            bkp_ext = collection + "." + action
        bkp_line = json.dumps(index_doc, separators = (',', ':')) + " " + json.dumps(doc, separators = (',', ':'))

        return bkp_ext, bkp_line

    def add_to_batch(self, log = None):
        if log:
            self.__log_lines.append(log)
        self.count += 1
        self.bson_size = 0

    def put_batch(self):
        Queue.put(self, (self.count, self.__log_lines, self.__batch))
        self.__batch = {}
        self.__log_lines = []
        self.count = 0
        self.bson_size = 0

    def get_batch(self, upsert = True):
        count, log_lines, batch = Queue.get(self)
        bkp_ext_lines = {}
        for collection in batch:
            if not collection in self.collections:
                self.collections[collection] = self.__db_database.get_collection(collection, write_concern = WriteConcern(w = self.__write_concern, fsync = self.__fsync))
            requests = []
            for action in batch[collection]:
                if action in ["none", "stat", "mark"]:
                    bkp_ext = collection + ".set"
                else:
                    bkp_ext = collection + "." + action
                if not bkp_ext in bkp_ext_lines:
                    bkp_ext_lines[bkp_ext] = []
                for index_doc, doc in batch[collection][action]:
                    if (self.__out_local and action in ["unset", "set", "addToSet", "insert"]) or (self.__stats_local and action in ["none", "stat"]):
                        bkp_ext_lines[bkp_ext].append(json.dumps(index_doc, separators = (',', ':')) + " " + json.dumps(doc, separators = (',', ':')))
                    if self.__out_db and action == "unset":
                        requests.append(UpdateOne(index_doc, {"$unset": doc}))
                    elif (self.__out_db and action == "set") or (self.__stats_db and action == "stat") or (action == "mark"):
                        requests.append(UpdateOne(index_doc, {"$set": doc}, upsert = upsert))
                    elif self.__out_db and action == "addToSet":
                        requests.append(UpdateOne(index_doc, {"$addToSet": doc}, upsert = upsert))
                    elif self.__out_db and action == "insert":
                        merged_doc = merge_dicts(index_doc, doc)
                        requests.append(InsertOne(merged_doc))
            if len(requests) > 0:
                try:
                    self.collections[collection].bulk_write(requests, ordered = self.__ordered)
                except BulkWriteError as bwe:
                    pprint(bwe.details)
        return count, log_lines, bkp_ext_lines

    def close(self):
        self.__db_client.close()