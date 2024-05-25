from hidden_jams.entity import EntityType
from hidden_jams.memgraph import GraphDB


db = GraphDB()
db.delete_all_of_entity_type(EntityType.ARTIST)
