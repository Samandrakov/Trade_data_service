import os
from pathlib import Path
from dataclasses import dataclass


@dataclass
class Config:
    DB_PATH: str = os.getenv('DB_PATH', 'data/high_tech_2024.db')
    DATA_DIR: str = os.getenv('DATA_DIR', 'data')
    OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', 'outputs')
    DEFAULT_LIMIT: int = 20
    FUZZY_THRESHOLD: float = 0.6
    PRODUCT_THRESHOLD: float = 0.5

    def __post_init__(self):
        Path(self.OUTPUT_DIR).mkdir(exist_ok=True)
        Path(self.DATA_DIR).mkdir(exist_ok=True)


config = Config()