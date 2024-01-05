import json
import faker
from models import Contact
from connect import connect
import pika

def generate_fake_contacts(num_contacts):
    fake = faker.Faker()
    contacts = []
    for _ in range(num_contacts):
        contact_data = {
            'full_name': fake.name(),
            'email': fake.email()
        }
        contacts.append(contact_data)
    return contacts

def save_contacts_to_db(contacts):
    for contact_data in contacts:
        contact = Contact(**contact_data)
        contact.save()

def send_messages_to_rabbitmq(contacts):
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='email_queue')

    for contact_data in contacts:
        contact = Contact.objects(**contact_data).first()
        if contact:
            message = {'contact_id': str(contact.id)}
            channel.basic_publish(exchange='', routing_key='email_queue', body=json.dumps(message))
    
    connection.close()

if __name__ == '__main__':
    num_fake_contacts = 5  
    fake_contacts = generate_fake_contacts(num_fake_contacts)
    
    save_contacts_to_db(fake_contacts)
    send_messages_to_rabbitmq(fake_contacts)





