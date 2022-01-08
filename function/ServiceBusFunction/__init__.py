import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    conn = psycopg2.connect(
    host="pj3-postgres-server.postgres.database.azure.com",
    database="techconfdb",
    user="azureuser@pj3-postgres-server",
    password="adminuser123!")
    
    cur = conn.cursor()
    try:
        # TODO: Get notification message and subject from database using the notification_id
        cur.execute('select message, subject from notification where id = %s;', (notification_id,))
        noti = cur.fetchone()
        logging.info('Get notification message and subject %s', noti)
        # TODO: Get attendees email and name        
        cur.execute('select email, first_name, last_name from attendee;')
        attendees = cur.fetchall()
        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            logging.info('Attendee: %s', attendee)
            email = attendee[0]
            message = Mail(
            from_email=os.environ['ADMIN_EMAIL_ADDRESS'],
            to_emails=email,
            subject=noti[1],
            plain_text_content=noti[0])

            sg = SendGridAPIClient(os.environ['SENDGRID_API_KEY'])
            sg.send(message)
            logging.info("Successfully sent %s", email)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        logging.info('The total number of attendees: %s', (cur.rowcount,))
        status = 'The total number of attendees notified :{}'.format(cur.rowcount)
        cur.execute('UPDATE NOTIFICATION SET STATUS = %s, COMPLETED_DATE = %s WHERE ID = %s', (status, datetime.utcnow(), notification_id,))
        conn.commit()
        logging.info("update successful")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        cur.close()
        conn.close()