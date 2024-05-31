from datetime import datetime

import mysql.connector
from server_interface import TripleStore

class MySQLTripleStore(TripleStore):
    def __init__(self, dbname, user, password, host, port):
        self.server_id="sql"
        self.conn = mysql.connector.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor()
        self.curr_log=1
        self.lim=1

    def query(self, subject, predicate):
        self.cur.execute("SELECT * FROM triples WHERE subject = %s and predicate = %s", (subject, predicate,))
        return self.cur.fetchall()
    def update_merge_log(self, server_id):
        last_merged_log_table=self.curr_log
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("SELECT timestamp FROM mergelog WHERE server_id = %s ORDER BY timestamp DESC LIMIT 1", (server_id,))
        last_merge_timestamp = self.cur.fetchone()
        if last_merge_timestamp:
            self.cur.execute("UPDATE mergelog SET timestamp = %s, last_merged_log_table = %s WHERE server_id = %s",
                            (current_timestamp, last_merged_log_table, server_id))
        else:
            self.cur.execute("INSERT INTO mergelog (server_id, timestamp, last_merged_log_table) VALUES (%s, %s, %s)",
                            (server_id, current_timestamp, last_merged_log_table))
        self.conn.commit()

    def fetch_logs(self,server_id):
        # server_id=server.server_type

        print("FETCHING THE LOGS FROM THE SQL SERVER")
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("SELECT timestamp FROM mergelog WHERE server_id = %s ORDER BY timestamp DESC LIMIT 1", (server_id,))
        last_merge_timestamp = self.cur.fetchone()
        self.cur.execute("SELECT last_merged_log_table FROM mergelog WHERE server_id = %s ORDER BY timestamp DESC LIMIT 1", (server_id,))
        last_merged_log_table=self.cur.fetchone()
        triplets = []
        if last_merge_timestamp:
            last_merge_timestamp = last_merge_timestamp[0]

        # Fetch logs from the database
            for i in range(last_merged_log_table, self.curr_log + 1):
                self.cur.execute("SELECT subject, predicate, object, timestamp FROM log{} WHERE timestamp > %s".format(i), (last_merge_timestamp,))
                logs = self.cur.fetchall()
                for log in logs:
                    triplet = (log[0], log[1], log[2],logs[3])  # Assuming log is a tuple (subject, predicate, object)
                    triplets.append(triplet)
            
            # Create a list to store triplets
            # Iterate over each log entry and create triplets
            
        else:
            for i in range(1, self.curr_log + 1):
                self.cur.execute("SELECT subject, predicate, object, timestamp FROM log{} WHERE timestamp > %s".format(i), (last_merge_timestamp,))
                logs = self.cur.fetchall()
                for log in logs:
                    triplet = (log[0], log[1], log[2],logs[3])  # Assuming log is a tuple (subject, predicate, object)
                    triplets.append(triplet)
        self.update_merge_log(server_id)
        # print("triplets ", triplets)
        return triplets

    def make_log_table(self):
        self.cur.execute("CREATE TABLE log" + str(self.curr_log) + "(subject VARCHAR(255) NOT NULL,object VARCHAR(255) NOT NULL,predicate VARCHAR(255),timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        self.conn.commit()

    def delete_entry(self,log_current,subject,predicate):
        self.cur.execute("DELETE FROM log"+ str(log_current)+ " WHERE subject = %s AND predicate = %s", (subject, predicate))
        self.conn.commit()

    
    def update(self, subject, predicate, obj,current_timestamp):
        
        self.cur.execute("SELECT COUNT(*) FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
        triples_result = self.cur.fetchone()[0]
        if (triples_result!=0):
            self.cur.execute("SELECT * FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
            log_entry = self.cur.fetchone()[4]
            print("log_entry: ", log_entry)
            if(log_entry==-1):
                self.cur.execute("SELECT COUNT(*) FROM log" + str(self.curr_log))
                val=self.cur.fetchone()[0]
                print("val: ", val)
                if(val>=self.lim):
                    self.curr_log+=1
                    self.make_log_table()
                
                
            else:
                self.delete_entry(log_entry,subject,predicate)
                self.cur.execute("SELECT COUNT(*) FROM log" + str(self.curr_log))
                val=self.cur.fetchone()[0]
                print("val: ", val)
                if(val>=self.lim):
                    self.curr_log+=1
                    self.make_log_table()
                
                
        else:
            self.cur.execute("SELECT COUNT(*) FROM log" + str(self.curr_log))
            val=self.cur.fetchone()[0]
            print("val: ", val)
            if(val>=self.lim):
                self.curr_log+=1
                self.make_log_table()
    
        if(triples_result!=0):
            self.cur.execute("UPDATES triples SET object=%s, timestamp=%s, log=%s WHERE subject=%s AND predicate=%s", (obj,current_timestamp,self.curr_log,subject,predicate))
        else:
            self.cur.execute("INSERT INTO triples (subject,predicate,object,timestamp,log) VALUES (%s, %s, %s, %s, %s)", (subject,predicate,obj,current_timestamp,self.curr_log))
    
        print("current log: ", self.curr_log)
        self.cur.execute("INSERT INTO log"+str(self.curr_log)+" (subject,predicate,object,timestamp) VALUES (%s, %s, %s, %s)", (subject, predicate, obj, current_timestamp))
        self.conn.commit()

    def merge(self, server_object):
        log_entries=server_object.fetch_logs(self.server_id)
        print(log_entries)
        for log_entry in log_entries:
            subject, predicate, obj, timestamp = log_entry
            self.update(subject,predicate,obj,timestamp)
        self.conn.commit()

    def load_tsv_file(self, file_path):
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("DELETE FROM mergelog")

        # # Delete all entries from the log table
        # self.cur.execute("DELETE FROM log")

        # # Delete all entries from the triples table
        self.cur.execute("DELETE FROM triples")
        self.make_log_table()
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for line in lines:
                data = line.strip().split(' ')
                if len(data) == 3:
                    subject, predicate, obj = data
                    self.cur.execute("SELECT COUNT(*) FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
                         # Subject-predicate pair already exists, update the object in the triples table
                    result = self.cur.fetchone()[0]  # Fetch the count result
                    if (result>0):
                        self.cur.execute("UPDATE triples SET object = %s, timestamp = %s WHERE subject = %s AND predicate = %s", (obj, current_timestamp, subject, predicate))
                    else:
                        self.cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)",
                                         (subject, predicate, obj, current_timestamp))
                else:
                    print(f"Ignore line: {line.strip()}. Not in the format 'subject predicate object'.")

        self.conn.commit()
    def drop_table(self,server_id):
        self.cur.execute("DROP TABLE IF EXISTS log" + str(server_id))
        self.conn.commit()          
    def close_the_server(self):
        for i in range(0, self.curr_log + 1):
            self.drop_table(i)
        self.cur.execute("DELETE FROM mergelog")
        self.conn.commit()
