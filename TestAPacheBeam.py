from apache_beam import  beam_runner_api_pb2
import  apache_beam as beam
print('welcome to apache beam ')

CUST_LIST=['ramesh','jagan','chaithu','saradi']

CUSTDIST_DATE={
        "cust_id": 1001,
        "first_name": "Laxmi",
        "last_name": "Reddy",
        "email": "laxmi.reddy@gmail.com",
        "phone": "9876543210",
        "city": "Hyderabad",
        "state": "Telangana",
        "country": "India",
        "account_type": "Savings",
        "balance": 150000.75,
        "status": "ACTIVE",
        "created_date": "2023-01-15"
    }
#Create pipliene object
#pipline1=beam.Pipeline()

#Create Pcollection
#pcollection1=beam.PCollection()

with beam.Pipeline() as p1:
    pcollection1=(
            p1
            |beam.Create(CUSTDIST_DATE)
            |beam.io.WriteToCsv('cust.csv')
    )