#!/usr/bin/python
import requests
import json
import threading, time
from kafka import KafkaAdminClient, KafkaProducer
from faker import Faker
import random



class Producer(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)
    self.stop_event = threading.Event()


  def stop(self):
    self.stop_event.set()


  def run(self):
    producer = KafkaProducer(bootstrap_servers='35.180.29.238:9092')
    print("about to send to Kafka")


    while not self.stop_event.is_set():
      print("sending event...")
      fake = Faker()
      agent = {
        "name": fake.name(),
        "disavowed": random.randint(0, 1)
      }

      # convert into JSON:
      agentJson = json.dumps(agent)
      b = bytes(agentJson, 'utf-8')
      producer.send('NOC_LIST', b)
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
