import threading
import time
from collections.abc import Hashable
from pathlib import Path
from threading import BoundedSemaphore, RLock

from database import Db


class SecureDb[KT: Hashable, VT]:
    def __init__(self, path: Path) -> None:
        self.__db = Db[KT, VT](path)

        self.__read_sema = BoundedSemaphore(10)
        self.__read_count = 0

        self.__write_lock = RLock()
        self.__is_writing = False

    def read(self, key: KT) -> VT | None:
        with self.__read_sema:
            while self.__is_writing:
                pass
            self.__read_count += 1
            thread = threading.current_thread()
            print(f"{thread.name} entering read")
            time.sleep(1)
            val = self.__db.read(key)
            print(f"{thread.name} exiting read")
            self.__read_count -= 1
            return val

    def write(self, key: KT, value: VT) -> bool:
        with self.__write_lock:
            while self.__read_count > 0:
                pass
            self.__is_writing = True
            thread = threading.current_thread()
            print(f"{thread.name} entering write")
            time.sleep(1)
            val = self.__db.write(key, value)
            print(f"{thread.name} exiting write")
            self.__is_writing = False
            return val

    def update(self, key: KT, value: VT) -> bool:
        with self.__write_lock:
            while self.__read_count > 0:
                pass
            self.__is_writing = True
            thread = threading.current_thread()
            print(f"{thread.name} entering update")
            time.sleep(1)
            val = self.__db.update(key, value)
            print(f"{thread.name} exiting update")
            self.__is_writing = False
            return val

    def delete(self, key: KT) -> bool:
        with self.__write_lock:
            while self.__read_count > 0:
                pass
            self.__is_writing = True
            thread = threading.current_thread()
            print(f"{thread.name} entering delete")
            time.sleep(1)
            val = self.__db.delete(key)
            print(f"{thread.name} exiting delete")
            self.__is_writing = False
            return val
