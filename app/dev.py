from backend.connectors.database import DBConfig
from backend.database.tables import *
from backend.connectors.database import TweeperDAO

engine = TweeperDAO(db=DBConfig(server="localhost",
                               port=5432,
                               user="localhost"))
Base.metadata.create_all()