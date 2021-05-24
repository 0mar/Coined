from sqlalchemy import Column, ForeignKey, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Coin(Base):
    __tablename__ = 'coin'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False, unique=True)

    def __str__(self):
        return f"Coin: {self.name}"

    def __repr__(self):
        return self.name


class Valuta(Base):
    __tablename__ = 'valuta'
    time = Column(Integer, primary_key=True)
    coin = Column(Integer, ForeignKey('coin.id'))
    price = Column(Float, nullable=False)
    volume = Column(Float)

    def __repr__(self):
        return f"{self.coin}@{self.time}:{self.price}"


engine = create_engine('sqlite:///prices.db')
Session = sessionmaker(bind=engine)
session = Session()


# Create all tables in the engine.
def setup(*coins):
    Base.metadata.create_all(engine)
    for name in coins:
        coin = session.query(Coin).filter(Coin.name==name).first()
        if not coin:
            coin = Coin(name=name)
            session.add(coin)
            session.commit()
            print("Added new coin %s"%coin)
