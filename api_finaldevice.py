import time
import socket
import json
import datetime

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey, DateTime, select, desc, asc
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from fastapi import FastAPI


# Define the URL
DB_ENGINE = 'postgresql://postgres:TLotjjRu1EeQJUIhxieM@containers-us-west-122.railway.app:5968/railway'


# Define the base class for the ORM
Base= declarative_base()


# Connect to the database engine 
engine= create_engine(DB_ENGINE, echo=True, future=True)
engine.connect()

# Create the FastAPI instance
app = FastAPI()

# Define the database schema for the different data types
# We create the tables on the server and define their structure
# We also define in some cases their relationship between them 
class OfficeSensor(Base):
    __tablename__ = 'officesensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    lights = Column(Boolean)
    someone = Column(Boolean)


class Warehouse(Base):
    __tablename__ = 'warehouse'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    power = Column(Boolean, nullable=False)
    temperature = Column(Float, nullable=False)
    
    
class BaySensor(Base):
    __tablename__ = 'baysensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, nullable=False)
    occupied = Column(Boolean, nullable=False)
    bay_id = Column(Integer, nullable=False)

# In this case the TransportBay sensor is related to BaySensor
# For this we introduce a foreign key with the id of BaySensor and define its relationship
class TransportBay(Base):
    __tablename__ = 'transportbay'
    id = Column(Integer, primary_key=True, autoincrement=True)
    baysensor_id = Column(Integer, ForeignKey('baysensor.id'), nullable=False)
    baysensor = relationship("BaySensor")
    general_datetime = Column(DateTime, nullable=False)
    general_power = Column(Boolean, nullable=False)
    

class MachineSensor(Base):
    __tablename__ = 'machinesensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime, nullable=False)
    machine_id = Column(Integer, nullable=False)
    working = Column(Boolean, nullable=False)
    faulty = Column(Boolean, nullable=False)

# In this case the MachineSensor sensor is related to Machinery
# For this we introduce a foreign key with the id of MachineSensor and we define their relationship
class Machinery(Base):
    __tablename__ = 'machinery'
    id = Column(Integer, primary_key=True, autoincrement=True)
    machinesensor_id = Column(Integer, ForeignKey('machinesensor.id'))
    machinesensor = relationship("MachineSensor")
    general_datetime = Column(DateTime, nullable=False)
    general_power = Column(Boolean, nullable=False)



# define endpoints
# we do a get (example in the slides) and in parentheses the route of the sensor that is
# the table that we define above, asks us that we can order in a way 
# ascend descendant and by dates
@app.get("/officesensor")
async def get_officesensor_data(order: str = "ascendant",
                                 init_date: datetime.date = None,
                                 end_date: datetime.date = None):

    with Session(engine) as session:
        # select the sensor from the table 
        statement = select(OfficeSensor)
        
        # the data is optional we can request it but it is not mandatory to show the data
        # so if it detects a parameter in start or end date
        # will show in the start case all the data from the start
        # and in the end the opposite    
        if init_date is not None:
            statement = statement.filter(OfficeSensor.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(OfficeSensor.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(OfficeSensor.datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(OfficeSensor.datetime.desc())
        # We execute the session and access the information
        # from the table and convert them to a dictionary
        results = session.execute(statement).scalars().all()
        
        return [r.__dict__ for r in results]


# Define the endpoint to retrieve warehouse data
@app.get("/warehouse")
async def get_warehouse_data(order: str = "ascendant",
                             init_date: datetime.date = None,
                             end_date: datetime.date = None):

    with Session(engine) as session:
        statement = select(Warehouse)
            
        if init_date is not None:
            statement = statement.filter(Warehouse.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(Warehouse.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(Warehouse.datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(Warehouse.datetime.desc())
        results = session.execute(statement).scalars().all()
    return [r.__dict__ for r in results]



    
@app.get("/baysensor")
async def get_baysensor_data(order: str = "ascendant",
                             init_date: datetime.date = None,
                             end_date: datetime.date = None):
    
    with Session(engine) as session:
        statement = select(BaySensor)
            
        if init_date is not None:
            statement = statement.filter(BaySensor.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(BaySensor.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(BaySensor.datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(BaySensor.datetime.desc())
        results = session.execute(statement).scalars().all()
    return [r.__dict__ for r in results]
    


# In the case of baysensor, it also asks us to enter an id
@app.get("/baysensor/{bay_id}")
async def get_baysensor_filter(bay_id: int,
                               order: str = "ascendant",
                               init_date: datetime.date = None,
                               end_date: datetime.date = None):
    
    with Session(engine) as session:
        statement = select(BaySensor)
            
        if init_date is not None:
            statement = statement.filter(BaySensor.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(BaySensor.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(BaySensor.datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(BaySensor.datetime.desc())
        results = session.execute(statement).scalars().all()
    # returns only if it matches the entered id     
    return [sensor for sensor in results if sensor.bay_id == bay_id]



@app.get("/transportbay")
def get_transportbay_data(order: str = "ascendant",
                      init_date: datetime.date = None,
                      end_date: datetime.date = None):
    
    # Open a new session
    
    with Session(engine) as session:
        # In this case we have a relationship between sensor tables, we must do a join with the
        # with the id that relates them 
        statement = select(TransportBay).join(BaySensor, TransportBay.baysensor_id == BaySensor.id)   

        if init_date is not None:
            statement = statement.filter(TransportBay.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(TransportBay.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(TransportBay.general_datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(TransportBay.general_datetime.desc())
        results = session.execute(statement).scalars().all()
    return [r.__dict__ for r in results]
    





@app.get("/machinesensor")
async def get_machinesensor_data(order: str = "ascendant",
                             init_date: datetime.date = None,
                             end_date: datetime.date = None):
    
    with Session(engine) as session:
        statement = select(MachineSensor)
            
        if init_date is not None:
            statement = statement.filter(MachineSensor.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(MachineSensor.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(MachineSensor.datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(MachineSensor.datetime.desc())
        results = session.execute(statement).scalars().all()
    return [r.__dict__ for r in results]



@app.get("/machinesensor/{machine_id}")
async def get_machinesensor_data(machine_id: int,
                                 order: str = "ascendant",
                                 init_date: datetime.date = None,
                                 end_date: datetime.date = None):
    
    with Session(engine) as session:
        statement = select(MachineSensor)
            
        if init_date is not None:
            statement = statement.filter(MachineSensor.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(MachineSensor.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(MachineSensor.datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(MachineSensor.datetime.desc())
        results = session.execute(statement).scalars().all()
    return [sensor for sensor in results if sensor.machine_id == machine_id]





@app.get("/machinery")
def get_transportbay_data(order: str = "ascendant",
                      init_date: datetime.date = None,
                      end_date: datetime.date = None):
    
    # Open a new session
    
    with Session(engine) as session:
        statement = select(Machinery).join(MachineSensor, Machinery.machinesensor_id  == MachineSensor.id)   

        if init_date is not None:
            statement = statement.filter(Machinery.datetime >= init_date)
        if end_date is not None:
            statement = statement.filter(Machinery.datetime <= end_date)
        if order == "ascendant":
            statement = statement.order_by(Machinery.general_datetime.asc())
        elif order == "descendant":
            statement = statement.order_by(Machinery.general_datetime.desc())
        results = session.execute(statement).scalars().all()
    return [r.__dict__ for r in results]  



