
import difflib
from typing import Dict, List, Tuple, Any


class FuzzySearch:
    """Класс для нечеткого поиска"""

    @staticmethod
    def search(
            search_term: str,
            mapping: Dict[int, str],
            threshold: float = 0.6,
            partial_match_score: float = 0.9,
            max_results: int = 5
    ) -> List[Tuple[int, str, float]]:
        """
        Нечеткий поиск в словаре

        Args:
            search_term: Строка для поиска
            mapping: Словарь для поиска (код: название)
            threshold: Порог схожести
            partial_match_score: Оценка для частичного совпадения
            max_results: Максимальное количество результатов

        Returns:
            Список кортежей (код, название, схожесть)
        """
        search_term = search_term.lower()
        matches = []

        for code, name in mapping.items():
            name_lower = str(name).lower()
            similarity = difflib.SequenceMatcher(None, search_term, name_lower).ratio()

            if similarity >= threshold:
                matches.append((code, name, similarity))
            elif search_term in name_lower:
                matches.append((code, name, partial_match_score))

        return sorted(matches, key=lambda x: x[2], reverse=True)[:max_results]

    @staticmethod
    def find_best_match(
            search_term: str,
            mapping: Dict[int, str],
            item_type: str,
            exact_search: bool = True
    ) -> Tuple[int, str, List[Tuple[int, str, float]]]:
        """
        Поиск лучшего соответствия с выводом подсказок

        Args:
            search_term: Строка для поиска
            mapping: Словарь для поиска
            item_type: Тип элемента ('country' или 'product')
            exact_search: Использовать точный поиск

        Returns:
            Кортеж (найденный код, название, все совпадения)
        """
        search_lower = search_term.lower()
        fuzzy_matches = []

        # Точный поиск
        if exact_search:
            for code, name in mapping.items():
                if str(name).lower() == search_lower:
                    return code, name, []

        # Нечеткий поиск
        fuzzy_matches = FuzzySearch.search(
            search_lower,
            mapping,
            threshold=0.6 if item_type == 'country' else 0.5
        )

        return None, None, fuzzy_matches