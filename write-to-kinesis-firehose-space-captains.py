import boto3
from faker import Faker
import random
import time
import json

DeliveryStreamName = 'captains-kfh'
client = boto3.client('firehose')
fake = Faker()

captains = [
    "Jean-Luc Picard",
    "James T. Kirk",
    "Han Solo",
    "Kathryn Janeway",
    "Malcoml Reynolds",
    "William Adama",
    "Bruno Global",
    "Turange Leela",
    "Jacob Keyes",
    "John Adams",
    "Wilhuff Tarkin",
    "Christopher Pike",
    "Creideiki",
    "David Bowman",
    "The Doctor",
    "Exeter",
    "John Robinson",
    "Adama",
    "Khan Noonien Singh"
];

record = {}
while True:
    record['user'] = fake.name();
    record['favoritecaptain'] = random.choice(captains);
    if record['favoritecaptain'] == "Jean-Luc Picard":
        record['rating'] = random.randint(5, 10);
    elif record['favoritecaptain'] == "James T. Kirk":
        record['rating'] = random.randint(3, 10):
    else: 
        record['rating'] = random.randint(1, 10);
    record['timestamp'] = time.time();
    response = client.put_record(
        DeliveryStreamName=DeliveryStreamName,
        Record={
            'Data': json.dumps(record)
        }
    )
    print('Record: \n' + str(record));
