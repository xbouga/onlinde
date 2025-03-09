import smtplib
import dns.resolver
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, make_msgid
from email.header import Header
import threading
import queue
import email.utils

NUM_THREADS = 50  # Fixer le nombre de threads à 100
BATCH_SIZE = 1    # Chaque batch contient 50 emails
SMTP_TIMEOUT = 60  # Set SMTP connection timeout to 60 seconds

def read_html_file(file_path):
    with open(file_path, "r", encoding="utf-8") as html_file:
        html_content = html_file.read()
    return html_content

def send_email_task(q, mx_server, sender_email, sender_name, subject, html_message, to_email, domain, success_log):
    while True:
        batch = q.get()
        if batch is None:
            break  # Arrêter le thread quand on reçoit "None"

        try:
            server = smtplib.SMTP(mx_server, timeout=SMTP_TIMEOUT)
            server.ehlo("mac.com")

            for recipient_email in batch:
                msg = MIMEMultipart()
                msg['From'] = formataddr((str(Header(sender_name, 'utf-8')), sender_email))
                msg['To'] = '<promo@elis.fi>'  # Fixed "To" header
                msg['Subject'] = Header(subject, 'utf-8')

                # Add headers to bypass spam filters
                msg['Message-ID'] = make_msgid(domain=domain)
                msg['Date'] = email.utils.formatdate(localtime=True)
                msg['Return-Path'] = sender_email  # Helps with DMARC alignment
                msg['X-Priority'] = '3'  # Normal priority
                msg['X-MSMail-Priority'] = 'Normal'
                msg['List-Unsubscribe'] = f"<mailto:unsubscribe@{domain}?subject=unsubscribe>"

                msg.attach(MIMEText(html_message, 'html', 'utf-8'))

                server.sendmail(sender_email, recipient_email, msg.as_string())
                with open(success_log, "a") as log_file:
                    log_file.write(f"{recipient_email}\n")
            print(f"Batch of {len(batch)} emails successfully sent.")

            server.quit()
        except Exception as e:
            print(f"Failed to send email batch: {e}")
        finally:
            q.task_done()

def prepare_and_send_batches(recipient_emails, subject, html_message, sender_email, sender_name, to_email):
    domain = recipient_emails[0].split('@')[1]
    mx_records = dns.resolver.resolve(domain, 'MX')
    mx_record = sorted(mx_records, key=lambda rec: rec.preference)[0]
    mx_server = str(mx_record.exchange).strip('.')

    q = queue.Queue()

    # Créer les threads pour envoyer les e-mails
    threads = []
    success_log = "success_log.txt"
    for _ in range(NUM_THREADS):
        thread = threading.Thread(target=send_email_task, args=(q, mx_server, sender_email, sender_name, subject, html_message, to_email, domain, success_log))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Ajouter les emails dans la queue par batch de 50
    for i in range(0, len(recipient_emails), BATCH_SIZE):
        batch = recipient_emails[i:i + BATCH_SIZE]
        q.put(batch)

    # Attendre que tous les emails soient envoyés
    q.join()

    # Envoyer un signal d'arrêt aux threads
    for _ in range(NUM_THREADS):
        q.put(None)
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    sender_email = "promo@elis.fi"
    sender_name = "Kundenteam"
    subject = "ADAC"
    html_message = read_html_file("message.html")

    with open("mails.txt", "r") as file:
        recipient_emails = [line.strip() for line in file.readlines()]

    to_email = ""

    # Envoyer les emails avec 100 threads fixes et batch de 50 emails
    prepare_and_send_batches(recipient_emails, subject, html_message, sender_email, sender_name, to_email)
