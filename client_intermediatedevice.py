import time
import socket
import simulator
import json

IP_ADDRESS = "127.0.0.1"
PORT = 45000
SEPARATOR = '#'

class IntermediateDevice():
   """
   Intermediate device class
   """
   def __init__(self, ip, port):
       """
       Class constructor. Create client socket
       """
       
       self.sensorSim = simulator.SensorSimulator()
       self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       self.socket.connect((ip, port))
       # We start the start of time, from our counter in this way we mark the beginning
       self.sensor_data = ""
       self.time0 = time.time()
      

   def run(self):
       """
       Client main task:
       Create random data of the sensors every 0.5 seconds. After 5 seconds,
       the intermediate device will send all the generated data to the final
       device
       """
       while True:
          self.sensorSim.simulate()
          self.sensor_data = self.sensor_data + json.dumps(self.sensorSim.sensors)

	      # Add final '#' at the end of the message
          self.sensor_data = self.sensor_data + SEPARATOR
		  
          # We do the calculation to make the shipment in the required time
          time.sleep(0.5)

          dt = time.time() - self.time0

          # Snd data to the final device every 5 seconds
          # We update the start counter, in the current time (after 0.5s)
          if dt > 5:
             self.sendData(self.sensor_data)
             self.sensor_data = ""
             self.time0 = time.time()
             
      

   def sendData(self, data):
       """
       Send data to the server socket
       """
       print("sending data to the server...")
       self.socket.send(data.encode('utf8'))


if __name__ == '__main__':
   client = IntermediateDevice(IP_ADDRESS, PORT)
   client.run()
