import subprocess
from datetime import datetime
from server_interface import TripleStore
from postgres_triple_store import MySQLTripleStore
from MongDB_store import MongoDBTripleStore
from hive_triple_store import HiveTripleStore

def start_mongodb_server():
    # Start MongoDB server using subprocess
    print("Starting MongoDB server...")
    subprocess.Popen(["mongod"])  # Adjust the command as per your MongoDB installation

def main():
    # Create MongoDB, MySQL, and Hive triple stores
    mongodb_triple_store = MongoDBTripleStore(dbname="triples", host="localhost", port=27017)
    dbname = "triples"
    user = "root"
    password = "Shishir@123"
    host = "localhost"
    port = 3306
    triple_store = MySQLTripleStore(dbname, user, password, host, port)
    hive_triple_store = HiveTripleStore()

    # Loading the TSV files
    mongodb_triple_store.load_tsv_file(r"C:\Users\shahi\OneDrive\Documents\data.txt")
    triple_store.load_tsv_file(r"C:\Users\shahi\OneDrive\Documents\data.txt")

    # Interactive terminal session
    while True:
        print("Choose a server to interact with:")
        print("1. MongoDB")
        print("2. MySQL")
        print("3. Hive")
        print("4. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            server = mongodb_triple_store
        elif choice == "2":
            server = triple_store
        elif choice == "3":
            server = hive_triple_store
        elif choice == "4":
            triple_store.close_the_server()
            mongodb_triple_store.close_the_server()
            hive_triple_store.close_the_server()
            print("Exiting...")
            break
        else:
            print("Invalid choice.")
            continue

        print("Choose an action:")
        print("1. Query")
        print("2. Update")
        print("3. Merge")
        print("4. Exit")

        action = input("Enter your choice: ")

        if action == "1":
            subject = input("Enter subject: ")
            predicate = input("Enter predicate: ")
            results = server.query(subject, predicate)
            print("Query Results:", results)
        elif action == "2":
            subject = input("Enter subject: ")
            predicate = input("Enter predicate: ")
            obj = input("Enter object: ")
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            server.update(subject, predicate, obj,current_timestamp)
            print("Update Successful")
        elif action == "3":
            server_id = input("Enter server ID (mongo/mysql/hive): ")
            if server_id == server.server_id:
                print("Invalid: Can't merge the server with itself")
                continue
            elif server_id == "mongo":
                server.merge(mongodb_triple_store)
                mongodb_triple_store.merge(server)
            elif server_id == "mysql":
                server.merge(triple_store)
                triple_store.merge(server)
            elif server_id == "hive":
                server.merge(hive_triple_store)
                hive_triple_store.merge(server)
            else:
                print("Invalid server ID")
                continue

            print("Merge Successful")
        elif action == "4":
            triple_store.close_the_server()
            mongodb_triple_store.close_the_server()
            print("Exiting...")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
