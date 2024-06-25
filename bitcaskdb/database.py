# a quick code read up of a https://git.mills.io/prologic/bitcask/src/branch/main/internal/index/index.go
# indicates the use of a radix tree as a space optimizing trie data structure  which are useful for 
# constructing associative arrays with keys that can be expressed as strings. 
# items represent [FileID,Offset,Size] 

import os
import pickle
import threading
import json
import time
from queue import Empty, Queue
from .radixtree import Item,RadixTree

class BitcaskDatabase:
    def __init__(self, db_dir="bitcask_db") -> None:
        self.db_dir = db_dir
        os.makedirs(self.db_dir, exist_ok=True)
       
        self.db_filename = "db-1"
        self.keys_filename = "keys.pkl"
        self.hash_idx_filename = "db_hash_idx.idx"
        self.metadata_file_db = os.path.join(self.db_dir, "db.meta")
        if not os.path.exists(self.metadata_file_db):
            os.open(self.metadata_file_db,os.O_WRONLY | os.O_CREAT)
        self.db_file_path = os.path.join(self.db_dir, self.db_filename)
        if not os.path.exists(self.db_file_path):
            os.open(self.db_file_path,os.O_WRONLY | os.O_CREAT)
        self.hash_idx_file = os.path.join(self.db_dir,self.hash_idx_filename)
        if not os.path.exists(self.hash_idx_file):
            os.open(self.hash_idx_file,os.O_WRONLY | os.O_CREAT)
        self.keys_file = os.path.join(self.db_dir, self.keys_filename)
        if not os.path.exists(self.keys_file):
            os.open(self.keys_file,os.O_WRONLY | os.O_CREAT)
        self.hash_index = RadixTree()
        self.hash_index.load_from_file(self.hash_idx_file)
        self.keys: dict[str, Item] = dict()
        
        self.metadata = {"db_file_size": 0, "db_file_offset": 0}
        self.hash_index_locks = {"reader": threading.RLock(), "writer": threading.Lock()}
        self._load()

        self.tombstone_queue = Queue()
        self.shutdown_event = threading.Event()
        
        self.deletion_worker_thread = threading.Thread(target=self._deletion_worker, daemon=True)
        self.shutdown_worker_thread = threading.Thread(target=self._shutdown_worker, daemon=True)
    
        self.deletion_worker_thread.start()
        self.shutdown_worker_thread.start()

    def _load(self):
        if os.path.exists(self.metadata_file_db):
            with open(self.metadata_file_db, 'r') as meta_file:
                metadata_content = meta_file.read()
                if metadata_content != "":
                    jsn = json.loads(metadata_content)
                    jsn["db_file_size"] = int(jsn["db_file_size"]) * 1024
                    self.metadata = jsn
        if os.path.exists(self.keys_file):
            if os.path.getsize(self.keys_file) > 0:
                with open(self.keys_file, 'rb') as f:
                    self.keys = pickle.load(f)

    def _save(self):
        with open(self.metadata_file_db, 'w', newline='') as metadata_file_db:
            json.dump(self.metadata, metadata_file_db)
        with self.hash_index_locks["writer"]:
            self.hash_index.save_to_file(self.hash_idx_file)
        with open(self.keys_file, 'wb') as f:
            pickle.dump(self.keys, f)

    def put(self, key: str, value: bytes):
        """Write a key-value pair to the database."""
        with self.hash_index_locks["writer"]:
            file_id = self.db_filename
            size = len(value)
            file_path = os.path.join(self.db_dir, file_id)
            with open(file_path, 'ab') as data_file:
                offset = data_file.tell()
                data_file.write(value)
                self.metadata["db_file_offset"] = offset + size
                item = Item(
                    fileid=int(self.db_filename.strip("db-")),
                    offset=offset,
                    size=size
                )
                self.hash_index.insert(item)
                self.keys[key] = item

    def get(self, key: str) -> bytes:
        """Read a value by key from the database."""
        with self.hash_index_locks["reader"]:
            item = self.keys.get(key)
            if item:
                found = self.hash_index.search(item)
                if found:
                    file_id, offset, size = found.fileid,found.offset,found.size
                    file_path = os.path.join(self.db_dir, "db-" + str(file_id))
                    with open(file_path, 'rb') as data_file:
                        data_file.seek(offset)
                        value = data_file.read(size)
                        value.decode('utf-8')
                        return value
            else:
                return None

    def delete(self, key: str):
        """Mark a key for deletion."""
        with self.hash_index_locks["writer"]:
            item = self.keys.pop(key, None)
            if item:
                self.hash_index.delete(item)
                self.tombstone_queue.put(item)

    def _deletion_worker(self):
        while not self.shutdown_event.is_set():
            try:
                item :Item = self.tombstone_queue.get(timeout=1)
                file_path = os.path.join(self.db_dir, f"db-{item.fileid}")
                self._delete_from_file(file_path, item.offset, item.size)
                self.tombstone_queue.task_done()
            except Empty:
                continue
            time.sleep(1) 

    def _shutdown_worker(self):
        while not self.shutdown_event.is_set():
            time.sleep(60)
            self._save()


    def _delete_from_file(self,file_path: str, offset: int, size: int):
        with open(file_path, 'r+b') as file:
            file.seek(offset)
            file.write(b' ' * size)

    def __del__(self):
        self.shutdown_event.set()
        self.deletion_worker_thread.join()
        self.shutdown_worker_thread.join()
        self._save()