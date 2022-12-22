from ast import literal_eval
from os import getenv

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


APP_NAME = "communication"
ENV = getenv("ENV", "PRODUCTION").lower()
HEADERS = {"Content-Type": "application/json"}
REDIS = {"HOST": getenv("REDIS_HOST"), "PORT": getenv("REDIS_PORT")}

COMM_POSTGRES = {
    "HOST": getenv("DB_HOST"),
    "PORT": getenv("DB_PORT"),
    "USER": getenv("COMMUNICATION_DB_USER"),
    "PASSWORD": getenv("COMMUNICATION_DB_PASSWORD"),
    "NAME": getenv("COMMUNICATION_DB_NAME"),
    "ENABLE_DB_READ_REPLICA": getenv("ENABLE_DB_READ_REPLICA"),
    "READ_REPLICA_DB_HOST": getenv("READ_REPLICA_DB_HOST"),
    "READ_REPLICA_DB_PORT": getenv("DB_PORT"),
}

BASE_ROUTE = getenv("BASE_ROUTE")