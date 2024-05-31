from datetime import datetime
from pymongo import MongoClient
from server_interface import TripleStore

class MongoDBTripleStore(TripleStore):
    def __init__(self, dbname, host='localhost', port=27017):
        self.server_id = "mongodb"
        self.client = MongoClient(host, port)
        self.db = self.client[dbname]
        self.curr_log = 1
        self.curr_lim = 1

    def query(self, subject, predicate):
        collection = self.db['triples']
        return collection.find({"subject": subject, "predicate": predicate})

    def update_merge_log(self, server_id):
        collection = self.db['mergelog']
        current_timestamp = datetime.now()
        last_merged_log_table = self.curr_log
        log_entry = {"server_id": server_id, "timestamp": current_timestamp, "last_merged_log_table": last_merged_log_table}
        collection.insert_one(log_entry)

    def fetch_logs(self, server_id):
        collection = self.db['mergelog']
        last_merge_log = collection.find_one({"server_id": server_id}, sort=[("timestamp", -1)])
        last_merge_timestamp = last_merge_log['timestamp'] if last_merge_log else datetime.min
        print(last_merge_timestamp)
        logs = []
        val=1
        if last_merge_log:
            val=last_merge_log['last_merged_log_table']
        for i in range(val, self.curr_log + 1):
            print("i ", i)
            collection_name = 'log{}'.format(i)
            print("collection_name ", collection_name)
            log_collection = self.db[collection_name]
            log_entries = log_collection.find()
            for x in log_entries:
                print("x: ", x)
                logs.append((x['subject'], x['predicate'], x['object'], x['timestamp']))
        self.update_merge_log(server_id)
        return logs

    def make_log_collection(self):
        collection_name = 'log{}'.format(self.curr_log)
        self.db.create_collection(collection_name)

    def delete_entry(self, log_current, subject, predicate):
        collection_name = 'log{}'.format(log_current)
        collection = self.db[collection_name]
        collection.delete_many({"subject": subject, "predicate": predicate})

    def update(self, subject, predicate, obj,current_timestamp):
        collection = self.db['triples']
        log_collection_name = 'log{}'.format(self.curr_log)
        log_collection = self.db[log_collection_name]
        # current_timestamp = datetime.now()
        triple_entry = {"subject": subject, "predicate": predicate, "object": obj, "timestamp": current_timestamp, "log": self.curr_log}
        log_entry = {"subject": subject, "predicate": predicate, "object": obj, "timestamp": current_timestamp}
        
        triples_result = collection.count_documents({"subject": subject, "predicate": predicate})
        if triples_result != 0:
            log_entry_result = collection.find_one({"subject": subject, "predicate": predicate}, {"log": 1})
            log_entry_log = log_entry_result.get("log", -1)
            if log_entry_log == -1:
                log_count = log_collection.count_documents({})
                if log_count >= self.curr_lim:
                    self.curr_log += 1
                    self.make_log_collection()
                    log_collection_name = 'log{}'.format(self.curr_log)
                    log_collection = self.db[log_collection_name]
                    log_collection.insert_one(log_entry)
                else:
                    log_collection.insert_one(log_entry)
            else:
                self.delete_entry(log_entry_log, subject, predicate)
                log_count = log_collection.count_documents({})
                if log_count >= self.curr_lim:
                    self.curr_log += 1
                    self.make_log_collection()
                    log_collection_name = 'log{}'.format(self.curr_log)
                    log_collection = self.db[log_collection_name]
                    log_collection.insert_one(log_entry)
                else:
                    log_collection.insert_one(log_entry)
        else:
            log_count = log_collection.count_documents({})
            if log_count >= self.curr_lim:
                self.curr_log += 1
                self.make_log_collection()
                log_collection_name = 'log{}'.format(self.curr_log)
                log_collection = self.db[log_collection_name]
                log_collection.insert_one(log_entry)
            else:
                log_collection.insert_one(log_entry)
            collection.insert_one(triple_entry)

    def merge(self, server_object):
        log_entries = server_object.fetch_logs(self.server_id)
        for log_entry in log_entries:
            subject, predicate, obj, timestamp = log_entry
            self.update(subject, predicate, obj, timestamp)

    def load_tsv_file(self, file_path):
        collection = self.db['triples']
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for line in lines:
                data = line.strip().split(' ')
                # print(data)
                if len(data) == 3:
                    subject, predicate, obj = data
                    current_timestamp = datetime.now()
                    triple_entry = {"subject": subject, "predicate": predicate, "object": obj, "timestamp": current_timestamp, "log": -1}
                    collection.insert_one(triple_entry)
                else:
                    print(f"Ignore line: {line.strip()}. Not in the format 'subject predicate object'.")

    def drop_collection(self, server_id):
        collection_name = "log" + str(server_id)
        self.db[collection_name].drop()
    def close_the_server(self):
        for i in range(self.curr_log + 1):
            self.drop_collection(i)
        collection =self.db['mergelog']
        result = collection.delete_many({})
        print("Number of documents deleted:", result.deleted_count)
        collection =self.db['triples']
        result = collection.delete_many({})
        print("Number of documents deleted:", result.deleted_count)
        