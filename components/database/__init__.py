from .database import Database
from .hooks import build_indexes_and_list_views
from components.cluster import cluster

db = Database(base="database", main_file="main.json", codec="msgpack")
db.cluster = cluster
