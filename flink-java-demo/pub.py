from kafka import KafkaProducer
 
producer = KafkaProducer(
    bootstrap_servers='192.168.6.163:9092'
)
 
producer.send('raw-measurements', b'Hello from praveen')
producer.flush()
 
print("Message sent")