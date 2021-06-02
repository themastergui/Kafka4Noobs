#!/usr/bin/python
import requests
import json
import threading, time
from kafka import KafkaAdminClient, KafkaProducer
from faker import Faker
from random import uniform
import random



class Producer(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
    self.stop_event = threading.Event()


  def stop(self):
    self.stop_event.set()


  def run(self):
    producer = KafkaProducer(bootstrap_servers='13.36.209.227:9092')
    print("about to send to Kafka")

    district_list = ["1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th", "11th", "12th", "13th", "14th", "15th", "16th", "17th", "18th", "19th", "20th"]

    def newpoint():
      return uniform(-180,180), uniform(-90, 90)

 

    while not self.stop_event.is_set():
      print("sending event...")
      fake = Faker()
      passengerId = random.randint(0, 1000000)
      key = {
        "account_id" : passengerId
      }
      
      payload = {
        "passenger_name": fake.name(),
        "passenger_id": passengerId,
        "pickup_coordinates" : newpoint(),
        "destination_coordinates": newpoint(),
        "pickup_district": random.choice(district_list),
        "destination_district": random.choice(district_list)
      }

      # convert into JSON:
      payloadJson = json.dumps(payload)
      keyJson = json.dumps(key)
      b = bytes(payloadJson, 'utf-8')
      k = bytes(keyJson, 'utf-8')
      producer.send('passenger_requests', value=b, key=k)
      time.sleep(3)

    producer.close()

def main():
  print("starting...")
  # Create 'my-topic' Kafka topic

  tasks = [
    Producer()
  ]


  # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
  for t in tasks:
    print("about to run task...")
    t.start()


  print("About to sleep...")
  time.sleep(1000)


  # Stop threads
  for task in tasks:
    task.stop()


  for task in tasks:
    task.join()

if __name__ == "__main__":
  main()
