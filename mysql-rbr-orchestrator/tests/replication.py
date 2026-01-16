import mysql.connector
import time
import uuid
import sys

class ReplicationTester:
    def __init__(self, master_cfg, replica_cfgs, db_name="repl_test_db"):
        self.master_cfg = master_cfg
        self.replica_cfgs = replica_cfgs
        self.db_name = db_name
        self.table_name = "items"
        self.test_payload = f"test_data_{uuid.uuid4().hex[:6]}"

    def setup_master(self):
        """Creates the database, table, and inserts test data on the Master."""
        print(f"[*] Connecting to Master ({self.master_cfg['port']})...")
        conn = mysql.connector.connect(**self.master_cfg)
        cursor = conn.cursor()
        
        print(f"[*] Creating database '{self.db_name}' and table...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        cursor.execute(f"USE {self.db_name}")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INT PRIMARY KEY,
                content VARCHAR(255)
            )
        """)
        
        print(f"[*] Inserting payload: {self.test_payload}")
        cursor.execute(f"INSERT INTO {self.table_name} (id, content) VALUES (1, %s)", (self.test_payload,))
        conn.commit()
        
        cursor.close()
        conn.close()

    def verify_replicas(self):
        """Checks each replica to see if the payload exists."""
        all_synced = True
        # Give replication a moment to breathe
        time.sleep(2) 

        for cfg in self.replica_cfgs:
            print(f"[*] Checking Replica on port {cfg['port']}...")
            try:
                conn = mysql.connector.connect(**cfg, database=self.db_name)
                cursor = conn.cursor()
                
                query = f"SELECT content FROM {self.table_name} WHERE content = %s"
                cursor.execute(query, (self.test_payload,))
                
                if cursor.fetchone():
                    print(f"    ✅ Success")
                else:
                    print(f"    ❌ Failure: Data not found")
                    all_synced = False
                    
                cursor.close()
                conn.close()
            except mysql.connector.Error as e:
                print(f"    ❌ Connection Error: {e}")
                all_synced = False
        
        return all_synced

    def cleanup(self):
        """Removes the test database from the Master (which syncs to replicas)."""
        print(f"[*] Cleaning up: Dropping database '{self.db_name}'...")
        conn = mysql.connector.connect(**self.master_cfg)
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
        conn.close()

def main():
    # Configuration
    m_cfg = {"host": "127.0.0.1", "port": 3306, "user": "root", "password": ""}
    r_cfgs = [
        {"host": "127.0.0.1", "port": 3307, "user": "root", "password": "rootpass"},
        {"host": "127.0.0.1", "port": 3308, "user": "root", "password": "rootpass"}
    ]

    tester = ReplicationTester(m_cfg, r_cfgs)

    try:
        tester.setup_master()
        if tester.verify_replicas():
            print("\n🎉 REPLICATION VERIFIED")
            exit_code = 0
        else:
            print("\n🚨 REPLICATION FAILED")
            exit_code = 1
    finally:
        tester.cleanup()
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()