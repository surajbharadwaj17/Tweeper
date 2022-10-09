from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.sql import func

Base = declarative_base()

# class TweetsRaw(Base):
#     __tablename__ = "t_tweets_raw"

# class CompanyTweets(Base):
#     __tablename__ = "t_company_tweets"
#     id = Column(String(), primary_key=True, nullable=False)
#     text = Column(String(), nullable=False)


class Weather(Base):
    __tablename__ = "t_weather_forecast_hourly"

    time_insert = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), primary_key=True, nullable=False)
    city = Column(String(), nullable=False, primary_key=True)
    data = Column(JSON)


