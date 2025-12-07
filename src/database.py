import sqlite3
import pandas as pd
from typing import Optional, List, Any
from contextlib import contextmanager


class DatabaseManager:
    """Менеджер базы данных"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для подключения к БД"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            yield conn
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            yield None
        finally:
            if conn:
                conn.close()

    def execute_query(
            self,
            query: str,
            params: List[Any] = None,
            fetch: bool = True
    ) -> Optional[pd.DataFrame]:
        """Выполнение SQL запроса"""
        with self.get_connection() as conn:
            if not conn:
                return None

            try:
                if fetch:
                    df = pd.read_sql_query(query, conn, params=params)
                    return df
                else:
                    cursor = conn.cursor()
                    cursor.execute(query, params or [])
                    conn.commit()
                    return None
            except Exception as e:
                print(f"Query execution error: {e}")
                return None