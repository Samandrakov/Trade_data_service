import argparse
import sys
from datetime import datetime

from src.extractor import TradeDataExtractor
from src.utils.validators import validate_args
from config import config


def create_parser() -> argparse.ArgumentParser:
    """Создание парсера аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description='Extract trade data from SQLite database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data_extractor.py -d 2024-12-01
  python data_extractor.py -c 36                 (Australia)
  python data_extractor.py -c "Turkey"           (fuzzy search)
  python data_extractor.py -p "animals live"     (fuzzy search)
  python data_extractor.py -d 2024-12-01 -c 36 -csv
  python data_extractor.py -d 2024-12-01 -c 36 -p 101
"""
    )

    # Основные параметры фильтрации
    parser.add_argument('-d', '--date', help='Date in YYYY-MM-DD format')
    parser.add_argument('-c', '--country', help='Country code or name')
    parser.add_argument('-p', '--product', help='Product code or name')

    # Параметры вывода
    parser.add_argument('-csv', '--to_csv', action='store_true',
                        help='Save result to CSV file')
    parser.add_argument('-o', '--output', help='Output CSV filename')

    # Параметры конфигурации
    parser.add_argument('-db', '--database', default=config.DB_PATH,
                        help=f'Database file path (default: {config.DB_PATH})')
    parser.add_argument('-data', '--data_dir', default=config.DATA_DIR,
                        help=f'Directory with code files (default: {config.DATA_DIR})')
    parser.add_argument('-l', '--limit', type=int, default=config.DEFAULT_LIMIT,
                        help=f'Display record limit (default: {config.DEFAULT_LIMIT})')

    # Параметры поиска и списков
    parser.add_argument('--list-countries', action='store_true',
                        help='List all available countries')
    parser.add_argument('--list-products', action='store_true',
                        help='List all available products')
    parser.add_argument('--search-country', help='Search for countries containing text')
    parser.add_argument('--search-product', help='Search for products containing text')

    return parser


def handle_list_operations(args, extractor):
    """Обработка операций со списками и поиском"""
    from src.utils.fuzzy_search import FuzzySearch

    if args.list_countries:
        print("AVAILABLE COUNTRIES (Code: English Name):")
        print("=" * 60)
        for code, name in sorted(extractor.country_codes.items()):
            print(f"{code:4d}: {name}")
        print(f"\nTotal: {len(extractor.country_codes)} countries")
        return True

    if args.list_products:
        print("AVAILABLE PRODUCTS (HS codes):")
        print("=" * 80)
        for code, name in sorted(extractor.hs_codes.items()):
            print(f"{code:6d}: {name}")
        print(f"\nTotal: {len(extractor.hs_codes)} product categories")
        return True

    if args.search_country:
        print(f"Searching for countries similar to: '{args.search_country}'")
        print("=" * 60)
        matches = FuzzySearch.search(
            args.search_country.lower(),
            extractor.country_codes,
            threshold=0.3
        )
        display_matches(matches, "countries")
        return True

    if args.search_product:
        print(f"Searching for products similar to: '{args.search_product}'")
        print("=" * 80)
        matches = FuzzySearch.search(
            args.search_product.lower(),
            extractor.hs_codes,
            threshold=0.3
        )
        display_matches(matches, "products")
        return True

    return False


def display_matches(matches, item_type):
    """Отображение результатов поиска"""
    if matches:
        print(f"\nTop matches:")
        for i, (code, name, similarity) in enumerate(matches, 1):
            width = 30 if item_type == "countries" else 50
            code_format = 4 if item_type == "countries" else 6
            print(f"{i:2d}. {code:{code_format}d}: {name:<{width}} "
                  f"(similarity: {similarity:.2f})")
    else:
        print(f"No similar {item_type} found")


def display_data(df, limit=10):
    """Отображение данных"""
    if df is None or df.empty:
        print("No data to display")
        return

    print("\n" + "=" * 120)
    print("TRADE DATA EXTRACTION RESULTS")
    print("=" * 120)

    # Настройка отображения pandas
    import pandas as pd
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 150)
    pd.set_option('display.max_rows', limit)

    # Отображаемые столбцы
    display_columns = ['date', 'flowtype', 'reporter_name', 'partner_name',
                       'cmdCode', 'product_description', 'qty', 'primaryvalue']
    available_columns = [col for col in display_columns if col in df.columns]

    if available_columns:
        # Описания столбцов
        descriptions = {
            'date': 'Transaction Date',
            'flowtype': 'Trade Flow Type (Import/Export)',
            'reporter_name': 'Reporter Country Name',
            'partner_name': 'Partner Country Name',
            'cmdCode': 'HS Product Code',
            'product_description': 'HS Product Description',
            'qty': 'Quantity',
            'primaryvalue': 'Trade Value (USD)'
        }

        print("\nColumn descriptions:")
        print("-" * 50)
        for col in available_columns:
            print(f"{col}: {descriptions.get(col, '')}")
        print("-" * 50)

        # Данные
        print(f"\n{df[available_columns].head(limit)}")

    print(f"\nTotal records: {len(df)}")

    # Сводная статистика
    if not df.empty:
        print("\nSUMMARY:")
        print("-" * 30)

        if 'flowtype' in df.columns:
            for flow_type, count in df['flowtype'].value_counts().items():
                print(f"{flow_type}: {count} records")

        if 'reporter_name' in df.columns:
            print(f"Unique reporter countries: {df['reporter_name'].nunique()}")

        if 'partner_name' in df.columns:
            print(f"Unique partner countries: {df['partner_name'].nunique()}")

        if 'primaryvalue' in df.columns and df['primaryvalue'].notna().any():
            print(f"Total trade value: ${df['primaryvalue'].sum():,.2f}")

        if 'qty' in df.columns and df['qty'].notna().any():
            print(f"Total quantity: {df['qty'].sum():,.2f}")


def main():
    """Основная функция приложения"""
    parser = create_parser()
    args = parser.parse_args()

    # Валидация аргументов
    is_valid, error_msg = validate_args(args)
    if not is_valid:
        print(error_msg)
        sys.exit(1)

    # Создание экстрактора
    extractor = TradeDataExtractor(args.database, args.data_dir)

    # Обработка операций со списками
    if handle_list_operations(args, extractor):
        sys.exit(0)

    # Вывод информации о фильтрах
    print(f"Extracting data from: {args.database}")
    print("-" * 50)

    filters_info = [
        (args.date, "Date filter"),
        (args.country, "Country filter"),
        (args.product, "Product filter")
    ]

    for value, label in filters_info:
        if value:
            print(f"{label}: {value}")

    print()

    # Извлечение данных
    df = extractor.extract_data(
        date=args.date,
        country=args.country,
        product=args.product
    )

    if df is not None:
        display_data(df, limit=args.limit)

        # Сохранение в CSV
        if args.to_csv and not df.empty:
            extractor.save_to_csv(df, args.output)
    else:
        print("Failed to extract data")


if __name__ == "__main__":
    main()