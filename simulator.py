import datetime
import random
'''
We create the class that will contain the sensors
'''
class SensorSimulator():
   """
   Class of the simulator of our device in the field
   """
   def __init__(self):
      """
       Initialization of required sensor data 
      """
      # Sensor transport list with the dictionaries it will contain
      transportBayDict = {"Baysensor":dict(),
                           "General":dict()}

      # Sensor machineri list with the dictionaries it will contain
      machineryDict = {"Machinesensor":dict(),
                        "General":dict()}

      # We create the definitive list that will contain all the dictionaries of all the sensors
      self.sensors = {"OfficeSensor": dict(),
                     "Warehouse":dict(),
                     "TransportBay":transportBayDict,
                     "Machinery":machineryDict
                     }

   def get_date_time(self):
      """
      Get data and time
      """
      return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S") # %Y-%m-%d_Hour_%H-%M-%S   %Y-%m-%dT%H:%M:%S
   
   def simulate(self):
      """
      Sensor data simulation
      """
      # Simulate Office Sensor
      # We create the content and fill in the dictionaries with the information from the sensors
      self.sensors["OfficeSensor"]["Datetime"] = self.get_date_time()
      self.sensors["OfficeSensor"]["Lights"]= random.choice([True, False])
      self.sensors["OfficeSensor"]["Someone"]= random.choice([True, False])

      # Simulate Warehouse
      # We create the content and fill in the dictionaries with the information from the sensors
      self.sensors["Warehouse"]["Datetime"] = self.get_date_time()
      self.sensors["Warehouse"]["Power"] = random.choice([True, False])
      self.sensors["Warehouse"]["Temperature"] = random.uniform(0, 100)

      # Simulate transport bay
      # We create the content and fill in the dictionaries with the information from the sensors
      self.sensors["TransportBay"]["Baysensor"]["Datetime"] = self.get_date_time()
      self.sensors["TransportBay"]["Baysensor"]["Occupied"] = random.choice([True, False])
      self.sensors["TransportBay"]["Baysensor"]["Bay_id"] = random.choice([1, 2])
      self.sensors["TransportBay"]["General"]["Datetime"] = self.get_date_time()
      self.sensors["TransportBay"]["General"]["Power"] = random.choice([True, False])

      # Simulate machinery
      # We create the content and fill in the dictionaries with the information from the sensors
      self.sensors["Machinery"]["Machinesensor"]["Datetime"] = self.get_date_time()
      self.sensors["Machinery"]["Machinesensor"]["MachineId"] = random.randrange(1, 4, 1)
      self.sensors["Machinery"]["Machinesensor"]["Working"] = random.choice([True, False])
      self.sensors["Machinery"]["Machinesensor"]["Faulty"] = random.choice([True, False])
      self.sensors["Machinery"]["General"]["Datetime"] = self.get_date_time()
      self.sensors["Machinery"]["General"]["Power"] = random.choice([True, False])




      
