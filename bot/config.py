from dataclasses import dataclass
from dotenv import dotenv_values, find_dotenv


@dataclass
class BaseConfig:
    CONNECTION_STRING: str # В виде "mongodb://localhost:27017"
    USERNAME: str
    PASSWORD: str
    AUTH_SOURCE: str
    TOKEN: str

    def get_config():
        return dotenv_values(find_dotenv())
