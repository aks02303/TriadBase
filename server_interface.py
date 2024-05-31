from abc import ABC, abstractmethod

class TripleStore(ABC):
    @abstractmethod
    def query(self, subject):
        pass

    @abstractmethod
    def update(self, subject, predicate, obj,current_timestamp):
        pass
    def fetch_logs(self):
        pass
    @abstractmethod
    def merge(self, server_object):
        pass
    def load_tsv_file(self, file_path):
        pass
