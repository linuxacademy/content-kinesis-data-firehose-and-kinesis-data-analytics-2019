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
    if random.randint(1,100) < 5:
        record['favoritecaptain'] = "Dex";
        record['rating'] = random.randint(6000,9000);
    else:
        record['favoritecaptain'] = random.choice(captains);
        record['rating'] = random.randint(1, 1000);
    record['timestamp'] = time.time();
    response = client.put_record(
        DeliveryStreamName=DeliveryStreamName,
        Record={
            'Data': json.dumps(record)
        }
    )
    print('Record: \n' + str(record));
