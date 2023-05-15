import time
import socket
import json
import datetime

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey, DateTime
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


# define the IP address and port to listen on
IP_ADDRESS = "127.0.0.1"
PORT = 45000

# Define the URL connection address of the database
DB_ENGINE = 'postgresql://postgres:TLotjjRu1EeQJUIhxieM@containers-us-west-122.railway.app:5968/railway'

# Set flags for saving received data as JSON or in a database, 
# para tener la opcion de guardar los datos en json o BD
SAVE_JSON_FILE = False
SAVE_DATABASE = True

# Set the separator character to use between data items 
# separator variable to be able to distinguish if the data received
# because sometimes the client sends us incomplete files
# It does not contain all the sensors in the 5s period, it does not make the complete shipment
SEPARATOR = '#'

# define the base class for the ORM
Base= declarative_base()


# Define the database schema for the different data types
# We create the tables on the server and define their structure
# We also define in some cases their relationship between them 
# We create the tables here because we have been able to verify that 
# through the connection with python we can create the tables if they do not exist
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
    
    


class FinalDevice():
   """
   Class to implement the final device.
   This class receives data from a TCP socket
   and saves it into a database or JSON file.
   """
   
   def __init__(self, ip, port):
       """
       Class to implement the final device.
       This class receives data from a TCP socket
       and saves it into a database or JSON file.
       """
       self.total_text_decoded = ""

       # Connect to the database engine  
       self.engine= create_engine(DB_ENGINE, echo=True, future=True)
       self.engine.connect()

       # create the database schema if it doesn't already exist this is possible thanks to SQLAlchemy
       Base.metadata.create_all(self.engine)

       # create a socket and bind it to the specified IP address and port
       self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       self.socket.bind((ip, port))
       self.socket.listen()
       self.conn, _= self.socket.accept()


       print("client connected!")


   def savejsonfile(self, data_items):
      """
      Save received data from the intermediate device into a json file
      """
      # Create the JSON file name using the current date and time
      # Json filename, create a header a title to the json file
      json_filename = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")+'.json'

      # Open the JSON file and write the received data into it
      with open(json_filename, 'w') as outfile:
         for elem in data_items:
            print('-----------')
            print(elem)
            print('-----------')
            json.dump(elem, outfile)
            outfile.write('\n')
            

   def saveDB(self, data_items):
      """
      Save received data from the intermediate device into a DATA BASE
      """
      # Use a database session to add the received data in variable data_item to the database.
      # we start session
      with Session (self.engine) as session:
         for elem in data_items:
            # Convert the JSON string into a Python dictionary.
            # To be able to process the data and be able to extract the information
            sensors_dict = json.loads(elem)
            print('-----------')
            print(sensors_dict)
            print('-----------')

            # Create OfficeSensor object and add it to the session.
            office_sensor_data = OfficeSensor(
            datetime=datetime.datetime.strptime(sensors_dict["OfficeSensor"]["Datetime"], '%Y-%m-%dT%H:%M:%S'),
            lights=sensors_dict["OfficeSensor"]["Lights"],
            someone=sensors_dict["OfficeSensor"]["Someone"])
            session.add(office_sensor_data)

            # Create Warehouse object and add it to the session.
            warehouse_data = Warehouse(
            datetime=sensors_dict["Warehouse"]["Datetime"],
            power=sensors_dict["Warehouse"]["Power"],
            temperature=sensors_dict["Warehouse"]["Temperature"])
            session.add(warehouse_data)

            # Create BaySensor and TransportBay objects and add them to the session
            bay_sensor_data = BaySensor(datetime=sensors_dict["TransportBay"]["Baysensor"]["Datetime"],
                                     occupied=sensors_dict["TransportBay"]["Baysensor"]["Occupied"],
                                     bay_id=sensors_dict["TransportBay"]["Baysensor"]["Bay_id"])
            session.add(bay_sensor_data)

            transport_bay_data = TransportBay(baysensor = bay_sensor_data,
                                           general_power=sensors_dict["TransportBay"]["General"]["Power"],
                                           general_datetime=sensors_dict["TransportBay"]["General"]["Datetime"])
            session.add(transport_bay_data)

            # Create MachineSensor and Machinery objects and add them to the session
            machine_sensor_data = MachineSensor(datetime = sensors_dict["Machinery"]["Machinesensor"]["Datetime"],
                                             machine_id=sensors_dict["Machinery"]["Machinesensor"]["MachineId"],
                                             working =sensors_dict["Machinery"]["Machinesensor"]["Working"],
                                             faulty = sensors_dict["Machinery"]["Machinesensor"]["Faulty"])
            session.add(machine_sensor_data)

            machinery_data = Machinery(machinesensor = machine_sensor_data,
                                    general_datetime = sensors_dict["Machinery"]["General"]["Datetime"],
                                    general_power = sensors_dict["Machinery"]["General"]["Power"])
            session.add(machinery_data)
            session.commit()
            

   def write_recv_data(self,text_decoded):
      """
      Method to write newly received data to the existing data
      We concatenate the information that we detected that the separator would be missing
      """
      self.total_text_decoded = self.total_text_decoded + text_decoded


   def new_data_available(self):
      """
      Method to check if new data is available (if the data contains a separator character)
      """
      if SEPARATOR in self.total_text_decoded:
         return True
      else:
         return False

   def read_data(self):
      """
      Method to read the next set of data items (separated by the separator character)
      Let's do the data treatment of the variable total_text_decoded
      In order to complete the shipments, if the information is complete
      """

      data_items = []
      terminated = False
      last_letter = self.total_text_decoded[-1]

      # Check if the last character of the data is the separator character
      # if the separator is there, the information is complete; if not, the file is incomplete
      # we extract the last element and compare it if it is equal to the separator we do the
      # send if not, the content is incomplete
      if last_letter == SEPARATOR:
         terminated = True
      
      # we do the separation by the separator, we separate the data sets if they exist
      data_split = self.total_text_decoded.rstrip(SEPARATOR).split(SEPARATOR)
      if len(data_split) > 0:
         # If the data is terminated (i.e., the last character is the separator),
         # clear the existing data and return the split data items
         if terminated:
            self.total_text_decoded = ""
            data_items = data_split
         # If the data is not terminated, update the existing data to the last split
         # item and return the variable with the data but less the last one which is where 
         # to cut and the one that is incomplete
         else:
            self.total_text_decoded = str(data_split[-1])
            data_items = data_split[:-1]
               
      return data_items
   
   def run(self):
      """
      we receive the data with the tcp connection 
      we start the variable where we want to save the data in 0
      """
      total_text_decoded = ""
      while True:
         # Receive data from the TCP socket and decode it
         text_decoded=self.conn.recv(6000).decode("utf-8")
         print('new data received')

         # Write the newly received data to the existing data
         self.write_recv_data(text_decoded)

         # Check if new data is available 
         if self.new_data_available() == True:
            print('new data available')

            # Read available data and we make the shipment if it contains information
            data_items = self.read_data()
            if len(data_items) > 0:
                
                # If saving JSON files is enabled, save the data items to a JSON file
                # Option also to be able to save in json file by default in False 
                # It has been implemented to be able to compare the information sent in DB, if it is correct or not 
                if SAVE_JSON_FILE == True:
                    self.savejsonfile(data_items)
                    
                # If saving to a database is enabled, save the data items to the database
                if SAVE_DATABASE == True:
                   self.saveDB(data_items)
                     


if __name__ == '__main__':
   # Create a FinalDevice object with the specified IP address and port and run the device
   server = FinalDevice(IP_ADDRESS, PORT)
   server.run()
   
