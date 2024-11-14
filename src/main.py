from faker import Faker
from datetime import datetime
import random
import time
from datetime import datetime, timezone
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import json

fake = Faker()

def generate_sales_transaction():
    user = fake.simple_profile()
    dados_fin = [fake.credit_card_number(),fake.credit_card_provider(),fake.credit_card_expire(),fake.credit_card_security_code()]
    return {
        "transactionID":fake.uuid4(),
        "productID": random.choice(['1', '2', '3', '4', '5', '6', '7']),
        "productName": random.choice(['Smartphone', 'Laptop', 'Tablet', 'Smartwatch', 'Headphone', 'Keyboard', 'Mouse','Video Game']),
        "productCategory":'Eletronics',
        "productPrice": round(random.uniform(500, 2000), 2),
        "productQuantity": random.randint(1, 5),
        "productBrand": random.choice([ 'Samsung','Xiaomi', 'Microsoft','Apple', 'Sony']),
        "currency": 'USD',
        "customerUsername": user['username'],
        "customerName": user['name'],
        "transactionDate": datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M:%S'),
        "paymentMethod": random.choice(['Credit Card', 'Debit Card']),
        "paymentCardNumber": fake.credit_card_number(),
        "paymentCardProvider": fake.credit_card_provider(),
        "paymentCardExpire": fake.credit_card_expire(),
        "paymentCardSecurityCode": fake.credit_card_security_code()
    }

def main():
    try:
        topic  = 'sales-transactions' 
        producer = SerializingProducer({
        'bootstrap.servers': 'localhost:29092',  
        'key.serializer': StringSerializer('utf_8'), 
        'value.serializer': StringSerializer('utf_8')
        })

        for i in range(0,10):
            try:
                transaction = generate_sales_transaction()
                producer.produce(
                     topic=topic,
                     key=transaction['transactionID'],
                     value=json.dumps(transaction),
                     on_delivery=delivery_report                   
                 )
                producer.flush() 
                time.sleep(1)
            except:
                print("Error")
    except:
        print("Error")

def delivery_report(err, msg):
    if err is not None:
        print(f"Não foi possível entregar o payload para a gravação {msg.key()}: {err}")
    else:
        print(f'Mensagem entregue com sucesso no tópico {msg.topic()} [{msg.partition()}]')

if __name__ == '__main__':
    main()