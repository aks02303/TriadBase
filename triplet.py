import datetime


class Triplet:
    def __init__(self, subject, predicate, object, timestamp):
        self.subject = subject
        self.predicate = predicate
        self.object = object

        # Timetamp is a datetime object
        # To store in a database, it can be converted to a isoformat string using isoformat() method

        # for isoformat string to datetime object see below
        if (isinstance(timestamp, str)):
            self.timestamp = datetime.datetime.fromisoformat(timestamp)
        elif(isinstance(timestamp, int)):
            self.timestamp = datetime.datetime.fromtimestamp(timestamp)
        else:
            self.timestamp = timestamp
