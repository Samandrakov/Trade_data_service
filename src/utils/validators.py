import argparse
import os
from pathlib import Path
from typing import Tuple


def validate_args(args: argparse.Namespace) -> Tuple[bool, str]:
    """Валидация аргументов командной строки"""
    if not os.path.exists(args.database):
        return False, f"Error: Database file '{args.database}' not found"

    if not any([args.date, args.country, args.product,
                args.list_countries, args.list_products,
                args.search_country, args.search_product]):
        return False, "Error: No arguments provided. Use --help for usage."

    return True, ""


def validate_date(date_str: str) -> bool:
    """Валидация формата даты"""
    try:
        from datetime import datetime
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False