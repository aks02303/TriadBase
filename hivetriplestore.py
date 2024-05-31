# from triplestore import TripleStore
# from postgres_triple_store_new import MySQLTripleStore
# from pyhive import hive
import subprocess
from triplet import Triplet
import datetime



class HiveTripleStore():
    def __init__(self):
        self.server_type = "hive"
        self.beeline = "beeline -u jdbc:hive2:// -e"
        # self.hive_connection = hive.connect(
        #     'localhost').cursor()
        # self.hive_connection.execute('DROP TABLE yagodataset')

        # self.hive_connection.execute(
        #     'CREATE TABLE  yagodataset(subject STRING, predicate STRING, object STRING) clustered by (object) into 2 buckets STORED AS ORC TBLPROPERTIES ("transactional"="true")')
        # self.hive_connection.execute(
        #     'INSERT INTO TABLE yagodataset SELECT subject, predicate, object FROM yagodataset_base')
        # query = "INSERT INTO TABLE yagodataset SELECT subject, predicate, object FROM yagodataset_base;"
        # query = "ALTER TABLE yagodataset ADD COLUMNS(timestp STRING)"
        # update yagodataset set timestp = unix_timestamp(CURRENT_TIMESTAMP())

        # print("done")
        # self.hive_connection.execute(
        #     'ALTER TABLE yagodataset ADD COLUMNS(timestp STRING)')

        # print(iso_str)
        # print('UPDATE yagodataset SET timestp = `' + iso_str + '`')
        
        # query = 'UPDATE yagodataset SET timestp = "' + iso_str + '";'
        # # self.hive_connection.execute('update yagodataset set dummy = "bel" where dummy = "hello"')

        # self.hive_connection.execute('SELECT * FROM yagodataset LIMIT 10')
        # print(self.hive_connection.fetchone())
    
    
        # current_datetime = datetime.datetime.now()
        # iso_str = current_datetime.isoformat()
        # query = 'select * from yagodataset limit 1;'
        # command = f"{self.beeline} '{query}'"
        # output = str(subprocess.check_output(command, shell=True))
        # print("output here")
        # print(output)
    def run_command(self, query_str):
        command = f"{self.beeline} '{query_str}'"
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        triplet_list = []

        while True:
            output = process.stdout.readline()
            # print(output)
            if output == b'' and process.poll() is not None:
                break
            if(not '<' in output.decode()):
                continue
            if output:
                elements = [element.strip() for element in output.decode().split("|")]
                elements = elements[1:-1]
                elements[-1] = int(elements[-1])
                if(len(elements) == 4):
                    triplet_list.append(Triplet(*elements))
                else: 
                    # print(elements)
                    triplet_list.append(elements)

        return triplet_list

    def query(self, subject,predicate):
        query_str = 'select * from yagodataset where subject = \"' + subject + '\";'
        triplet_list = self.run_command(query_str=query_str)
        for triplet in triplet_list:
            print("subject: " + triplet.subject + ", predicate: " + triplet.predicate + ", object: " + triplet.object + ", timestp: " + str(triplet.timestamp))
        return triplet_list

    def update(self, subject, predicate, object,currentTimeStamp):
        query_str = 'delete from yagodataset where subject = \"' + subject + '\" and predicate = \"' + predicate + '\";'
        self.run_command(query_str)

        current_datetime = datetime.datetime.now()
        unix_num = current_datetime.timestamp()


        query_str = 'INSERT INTO TABLE yagodataset VALUES(\"' + subject + '\", \"' + predicate + '\", \"' + object + '\",' + str(unix_num) +  ');'
        self.run_command(query_str)
        query_str = 'INSERT INTO TABLE log_table VALUES(\"' + subject + '\", \"' + predicate + '\", \"' + object + '\",' + str(unix_num) + ');'
        self.run_command(query_str)

        return
    

    def update_with_timestamp(self, subject, predicate, object, timestamp):
        query_str = 'delete from yagodataset where subject = \"' + \
            subject + '\" and predicate = \"' + predicate + '\";'
        self.run_command(query_str)

        # current_datetime = datetime.datetime.now()
        unix_num = timestamp.timestamp()

        query_str = 'INSERT INTO TABLE yagodataset VALUES(\"' + subject + \
            '\", \"' + predicate + '\", \"' + \
            object + '\",' + str(unix_num) + ');'
        self.run_command(query_str)
        query_str = 'INSERT INTO TABLE log_table VALUES(\"' + subject + '\", \"' + \
            predicate + '\", \"' + object + '\",' + str(unix_num) + ');'
        self.run_command(query_str)

        return


    #timestp in datetime obnject format
    def update_if_older(self, subject, predicate, object, timestamp):
        unix_num = timestamp.timestamp()
        query_str = 'select * from yagodataset where subject = \"' + subject + '\" and predicate = \"' + predicate + '\" and timestp >=' + str(unix_num) + ';'
        triplet_list = self.run_command(query_str)

        if(len(triplet_list) > 0): return

        self.update_with_timestamp(subject, predicate, object, timestamp)

        return


    def fetch_logs(self, server_id):
        current_datetime = datetime.datetime.now()
        unix_num = current_datetime.timestamp()

        query_str = 'select * from merge_table where server = \"<' + server_id + '>\";'
        doublet_list = self.run_command(query_str)
        # print(doublet_list)
        prev_merge = int(doublet_list[0][1])

        query_str = 'select * from log_table where timestp > ' + str(prev_merge) + ';'
        triplet_list = self.run_command(query_str)
        
        listoflist = []
        for triplet in triplet_list:
            listoflist.append([triplet.subject, triplet.predicate, triplet.object, triplet.timestamp])
        
        query_str = 'update merge_table set timestp = ' + str(unix_num) + ' where server = \"' + server_id + '\";'
        self.run_command(query_str)

        return listoflist

    def merge(self, server_object):
        tuples_list = server_object.fetch_logs(self.server_type)
        for tuple  in tuples_list:
            # print(tuple)
            self.update_if_older(*tuple)
        

        
        


# myhive = HiveTripleStore()
# myhive.update("<keshav>", "<studiesIn>", "<iiitb>")
# print(myhive.query('<keshav>'))
