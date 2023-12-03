import database as db

def process_message(topic, data):
    db.insert_raw(topic, data)



