import base64
import os.path
import pymysql

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Db:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = pymysql.connect(
            host=self.host, user=self.user, password=self.password, db=self.database
        )
        self.cursor = pymysql.cursors.Cursor(self.connection)


class Query:
    def __init__(self):
        self.db = Db("localhost", "root", "admin", "melichallengecamilo")

    def get_mail(self, date_email, from_email, subject_email):
        query = f"SELECT * FROM mails WHERE dateMail='{date_email}' AND fromMail='{from_email}' AND subjectMail='{subject_email}';"
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row

    def create_mail(self, date_email, from_email, subject_email):
        query = f"INSERT INTO mails(dateMail, fromMail, subjectMail) VALUES('{date_email}', '{from_email}', '{subject_email}');"
        self.db.cursor.execute(query)


class Mail:
    """
    qué hace
    """

    def store(self, date_email, from_email, subject_email):
        """
        descripción
        parámetros, tipo, descripción y ejemplo
        """
        query = Query()

        mail = query.get_mail(date_email, from_email, subject_email)
        if mail:
            return False

        query.create_mail(date_email, from_email, subject_email)
        query.db.connection.commit()
        return True

    def get_body(self, message):
        body = message["payload"]["parts"][0]["body"]["data"]
        body = body.replace("-", "+").replace("_", "/")
        decoded_body = base64.b64decode(body)
        formatted_body = str(decoded_body).lower()
        return formatted_body

    def get_headers(self, message):
        new_headers = {}
        headers = message["payload"]["headers"]
        for header in headers:
            if header["name"] == "Date":
                new_headers["date_email"] = header["value"]
            if header["name"] == "To":
                new_headers["to_email"] = header["value"]
            if header["name"] == "From":
                new_headers["from_email"] = header["value"]
            if header["name"] == "Subject":
                new_headers["subject_email"] = header["value"]
        return new_headers


class Gmail:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/gmail.readonly"]

        credentials = self._validate_credentials()
        self.service = build("gmail", "v1", credentials=credentials)

    def _validate_credentials(self):
        credentials = None
        if os.path.exists("token.json"):
            credentials = Credentials.from_authorized_user_file(
                "token.json", self.scopes
            )

        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", self.scopes
                )
                credentials = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(credentials.to_json())
        return credentials

    def get_messages(self):
        result = self.service.users().messages().list(userId="me").execute()
        return result["messages"]

    def get_message(self, message):
        result = (
            self.service.users().messages().get(userId="me", id=message["id"]).execute()
        )
        return result


def main():
    gmail = Gmail()
    mail = Mail()

    try:
        messages = gmail.get_messages()
        counter_total = 0
        counter_new = 0

        for msg in messages:
            message = gmail.get_message(msg)
            body = mail.get_body(message)
            headers = mail.get_headers(message)

            if "devops" in body:
                new = mail.store(
                    headers["date_email"],
                    headers["from_email"],
                    headers["subject_email"],
                )
                counter_total += 1
                if new:
                    counter_new += 1
        print(
            f"{counter_total} mails identified from a total of {len(messages)} mails. {counter_new} new records stored in database!"
        )

    except HttpError as error:
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    main()
