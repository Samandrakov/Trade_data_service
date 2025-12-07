import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


class FileHandler:
    """Класс для работы с файлами"""

    @staticmethod
    def load_country_codes(csv_path: Path) -> Dict[int, str]:
        """Загрузка кодов стран из CSV файла"""
        country_codes = {}

        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, encoding='utf-8')
                for _, row in df.iterrows():
                    if pd.notna(row['m49_code']) and pd.notna(row['country_name_en']):
                        try:
                            country_codes[int(row['m49_code'])] = row['country_name_en']
                        except (ValueError, TypeError):
                            continue
                print(f"Loaded {len(country_codes)} country codes from CSV")
            except Exception as e:
                print(f"Error loading country codes from CSV: {e}")
        else:
            print(f"Country codes CSV not found at: {csv_path}")

        return country_codes

    @staticmethod
    def load_hs_codes(json_path: Path) -> Dict[int, str]:
        """Загрузка HS кодов из JSON файла"""
        hs_codes = {}

        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for item in data.get('results', []):
                    item_id, text = item.get('id', ''), item.get('text', '')
                    if item_id and text and item_id != 'TOTAL':
                        try:
                            hs_codes[int(item_id.replace('.', ''))] = text
                        except (ValueError, TypeError):
                            continue
                print(f"Loaded {len(hs_codes)} HS codes from JSON")
            except Exception as e:
                print(f"Error loading HS codes from JSON: {e}")
        else:
            print(f"HS codes JSON not found at: {json_path}")

        return hs_codes

    @staticmethod
    def save_dataframe(
            df: pd.DataFrame,
            filename: str,
            column_descriptions: Dict[str, str]
    ) -> bool:
        """Сохранение DataFrame в CSV с описаниями"""
        try:
            # Сохранение данных
            df.to_csv(filename, index=False, encoding='utf-8')

            # Сохранение описаний
            desc_filename = filename.replace('.csv', '_columns.txt')
            with open(desc_filename, 'w', encoding='utf-8') as f:
                f.write("COLUMN DESCRIPTIONS\n===================\n\n")
                for col in df.columns:
                    desc = column_descriptions.get(col, '[No description available]')
                    f.write(f"{col}: {desc}\n")
                f.write(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total records: {len(df)}\n")

            return True
        except Exception as e:
            print(f"File save error: {e}")
            return False