import json
import time
import pika
from models import Contact
from connect import connect
import signal

def send_email_stub(contact_id):
    print(f"Simulating sending email to contact with ID: {contact_id}")
    time.sleep(2)  

def callback(ch, method, properties, body):
    message = json.loads(body)
    contact_id = message['contact_id']

    contact = Contact.objects(id=contact_id).first()
    if contact:
        send_email_stub(contact_id)
        contact.message_sent = True
        contact.save()

def exit_program(signum, frame):
    print('\nExiting program...')
    exit(0)

if __name__ == '__main__':
    # connect() 

    signal.signal(signal.SIGINT, exit_program)  # Handle Ctrl+C to exit the program

    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='email_queue')

    channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=True)

    print('Consumer is waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
