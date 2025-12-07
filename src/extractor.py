import pandas as pd
from typing import Optional, Dict, Any
from pathlib import Path

from .database import DatabaseManager
from .utils.fuzzy_search import FuzzySearch
from .utils.file_handlers import FileHandler
from config import config


class TradeDataExtractor:
    """Класс для извлечения торговых данных"""

    def __init__(self, db_path: str = None, data_dir: str = None):
        self.db_path = db_path or config.DB_PATH
        self.data_dir = Path(data_dir or config.DATA_DIR)

        self.db_manager = DatabaseManager(self.db_path)
        self.country_codes = self._load_country_codes()
        self.hs_codes = self._load_hs_codes()

        # Загрузка кодов по умолчанию при необходимости
        self._load_default_codes()

    def _load_country_codes(self) -> Dict[int, str]:
        """Загрузка кодов стран"""
        csv_path = self.data_dir / 'countries_codes.csv'
        return FileHandler.load_country_codes(csv_path)

    def _load_hs_codes(self) -> Dict[int, str]:
        """Загрузка HS кодов"""
        json_path = self.data_dir / 'H5.json'
        return FileHandler.load_hs_codes(json_path)

    def _load_default_codes(self):
        """Загрузка кодов по умолчанию"""
        if not self.country_codes:
            print("Warning: Could not load country codes. Using default codes.")
            self.country_codes = {792: 'Turkey', 643: 'Russian Federation'}

        if not self.hs_codes:
            print("Warning: Could not load HS codes. Using default codes.")
            self.hs_codes = {8401: 'Nuclear reactors'}

    def _get_code(
            self,
            search_term: str,
            mapping: Dict[int, str],
            item_type: str
    ) -> Optional[int]:
        """Получение кода по текстовому запросу"""
        if search_term.isdigit():
            return int(search_term)

        # Поиск лучшего соответствия
        code, name, matches = FuzzySearch.find_best_match(
            search_term, mapping, item_type
        )

        if code is not None:
            return code

        # Обработка нечетких совпадений
        if matches:
            print(f"{item_type.capitalize()} '{search_term}' not found exactly. Did you mean:")
            for i, (match_code, match_name, similarity) in enumerate(matches, 1):
                print(f"  {i}. {match_code}: {match_name} (similarity: {similarity:.2f})")

            if matches:
                match_code, match_name, _ = matches[0]
                print(f"\nUsing: {match_name} (code: {match_code})")
                return match_code

        print(f"{item_type.capitalize()} '{search_term}' not found.")
        print(f"Use --list-{item_type}s to see all available {item_type}s")
        return None

    def extract_data(
            self,
            date: str = None,
            country: str = None,
            product: str = None
    ) -> Optional[pd.DataFrame]:
        """Извлечение данных по заданным критериям"""
        query = """
        SELECT date, flowtype, ReporterCode, PartnerName, cmdCode, qty, primaryvalue
        FROM hightech_2024
        WHERE 1=1
        """

        params = []
        conditions_added = False

        # Построение условий запроса
        if date:
            query += " AND date = ?"
            params.append(date)
            conditions_added = True

        if country:
            country_code = self._get_code(country, self.country_codes, 'country')
            if country_code is None:
                return pd.DataFrame()
            query += " AND ReporterCode = ?"
            params.append(country_code)
            conditions_added = True

        if product:
            product_code = self._get_code(product, self.hs_codes, 'product')
            if product_code is None:
                return pd.DataFrame()
            query += " AND cmdCode = ?"
            params.append(product_code)
            conditions_added = True

        if not conditions_added:
            print("Error: At least one filter must be specified (date, country, or product)")
            print("Use --date, --country, or --product option")
            return pd.DataFrame()

        # Выполнение запроса
        df = self.db_manager.execute_query(query, params)

        if df is not None and not df.empty:
            # Преобразование типов и добавление названий
            df = self._enrich_dataframe(df)

        return df

    def _enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обогащение DataFrame дополнительной информацией"""
        # Преобразование типов
        df['ReporterCode'] = pd.to_numeric(df['ReporterCode'], errors='coerce')
        df['cmdCode'] = pd.to_numeric(df['cmdCode'], errors='coerce')

        # Добавление названий
        df['reporter_name'] = df['ReporterCode'].map(self.country_codes)
        df['product_description'] = df['cmdCode'].map(self.hs_codes)
        df['partner_name'] = df['PartnerName'].map(self.country_codes)

        # Заполнение отсутствующих значений
        df['reporter_name'] = df['reporter_name'].fillna(df['ReporterCode'].astype(str))
        df['product_description'] = df['product_description'].fillna(df['cmdCode'].astype(str))
        df['partner_name'] = df['partner_name'].fillna(df['PartnerName'])

        return df

    def save_to_csv(self, df: pd.DataFrame, filename: str = None) -> bool:
        """Сохранение данных в CSV"""
        if df is None or df.empty:
            print("No data to save")
            return False

        # Генерация имени файла
        from datetime import datetime
        filename = filename or f"{config.OUTPUT_DIR}/trade_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Описания столбцов
        column_descriptions = {
            'date': 'Transaction Date',
            'flowtype': 'Trade Flow Type (Import/Export)',
            'ReporterCode': 'Reporter Country Code',
            'reporter_name': 'Reporter Country Name',
            'PartnerName': 'Partner Country Code',
            'partner_name': 'Partner Country Name',
            'cmdCode': 'HS Product Code',
            'product_description': 'HS Product Description',
            'qty': 'Quantity',
            'primaryvalue': 'Trade Value (USD)'
        }

        success = FileHandler.save_dataframe(df, filename, column_descriptions)

        if success:
            print(f"Data saved to: {filename}")
            print(f"Records count: {len(df)}")

        return success