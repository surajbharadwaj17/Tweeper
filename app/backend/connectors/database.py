from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import *
from database.tables import *

class DBConfig:
    def __init__(self,
                server:str, 
                port:int,
                user:str,
                pwd:str,
                db:str):
        self.server = server
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

    @property
    def connection_string(self):
        return f"postgresql://{self.user}:{self.pwd}@{self.server}:{self.port}/{self.db}"
        


class TweeperDAO:
    def __init__(self, db:DBConfig) -> None:
        self.conn_str = db.connection_string

    def get_engine(self):
        return create_engine(self.db.connection_string).connect()

    def _init_tables(self):
        pass

    