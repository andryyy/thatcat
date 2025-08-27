from .database import Database
from components.cluster import cluster

db = Database(base="database", main_file="main.json", codec="msgpack")
db.set_cluster(cluster, replicate_changes=True)
