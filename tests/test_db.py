import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from pytest import approx

from securedb import SecureDb


@pytest.fixture
def db(tmp_path: Path) -> SecureDb[Any, Any]:
    return SecureDb(tmp_path / "test.db")


def test_write(db: SecureDb[Any, Any]):
    start = time.perf_counter()
    db.write("spam", "eggs")
    end = time.perf_counter()
    assert end - start == approx(1, 0.1)


def test_read(db: SecureDb[Any, Any]):
    start = time.perf_counter()
    db.read("spam")
    end = time.perf_counter()
    assert end - start == approx(1, 0.1)


def test_write_while_read(db: SecureDb[Any, Any]):
    start = time.perf_counter()
    with ThreadPoolExecutor() as executor:
        executor.submit(db.read, "spam")
        executor.submit(db.write, "eggs", "bacon")
    end = time.perf_counter()
    assert end - start == approx(2, 0.1)


def test_read_while_write(db: SecureDb[Any, Any]):
    start = time.perf_counter()
    with ThreadPoolExecutor() as executor:
        executor.submit(db.write, "spam", "eggs")
        executor.submit(db.read, "spam")
    end = time.perf_counter()
    assert end - start == approx(2, 0.1)


def test_parallel_read(db: SecureDb[Any, Any]):
    start = time.perf_counter()
    with ThreadPoolExecutor() as executor:
        for _ in range(10):
            executor.submit(db.read, "spam")
    end = time.perf_counter()
    assert end - start == approx(1, 0.1)


def test_all(db: SecureDb[Any, Any]):
    start = time.perf_counter()
    with ThreadPoolExecutor() as executor:
        for _ in range(10):
            executor.submit(db.read, "spam")
        executor.submit(db.write, "spam", "eggs")
        for _ in range(10):
            executor.submit(db.read, "spam")
    end = time.perf_counter()
    assert end - start == approx(3, 0.1)
