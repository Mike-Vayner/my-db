import pickle
from collections.abc import Hashable
from pathlib import Path


class Db[KT: Hashable, VT]:
    def __init__(self, path: Path) -> None:
        self.__path = path
        self.__db: dict[KT, VT] = {}
        with path.open("wb+") as db:
            try:
                self.__db |= pickle.load(db)
            except EOFError:
                pickle.dump(self.__db, db)

    def read(self, key: KT) -> VT | None:
        return self.__db.get(key)

    def write(self, key: KT, value: VT) -> bool:
        if key in self.__db:
            return False
        self.__db[key] = value
        self._save_dict()
        return True

    def update(self, key: KT, value: VT) -> bool:
        if key not in self.__db:
            return False
        self.__db[key] = value
        self._save_dict()
        return True

    def delete(self, key: KT) -> bool:
        if key not in self.__db:
            return False
        del self.__db[key]
        self._save_dict()
        return True

    def _save_dict(self):
        with self.__path.open("wb") as file:
            pickle.dump(self.__db, file)
