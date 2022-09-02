import base64
import os.path
import pymysql

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Db:
    """
    This class object allows the connection to the database.
    """

    def __init__(self, host, user, password, database):
        """
        Function that receives as arguments the data to connect to the MySQL database.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = pymysql.connect(
            host=self.host, user=self.user, password=self.password, db=self.database
        )
        self.cursor = pymysql.cursors.Cursor(self.connection)


class Query:
    """
    This class object executes statements on the database.
    """

    def __init__(self):
        """
        Function that sends the data to connect to the MySQL database.
        """
        self.db = Db("localhost", "root", "admin", "melichallengecamilo")

    def get_mail(self, date_email, from_email, subject_email):
        """
        Function that queries in the database, the record with the data received as arguments.
        Returns the data.
        """
        query = f"SELECT * FROM mails WHERE dateMail='{date_email}' AND fromMail='{from_email}' AND subjectMail='{subject_email}';"
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row

    def create_mail(self, date_email, from_email, subject_email):
        """
        Function that inserts into the database, a new record with the data received as arguments.
        """
        query = f"INSERT INTO mails(dateMail, fromMail, subjectMail) VALUES('{date_email}', '{from_email}', '{subject_email}');"
        self.db.cursor.execute(query)


class Mail:
    """
    This class object consults information from Gmail and obtains mails data that meet the condition to store in the database.
    """

    def store(self, date_email, from_email, subject_email):
        """
        Function that calls the Query class, validates if the mail already exists in the database and returns False.
        If not, it inserts record into the database and returns True.
        """
        query = Query()

        mail = query.get_mail(date_email, from_email, subject_email)
        if mail:
            return False

        query.create_mail(date_email, from_email, subject_email)
        query.db.connection.commit()
        return True

    def get_body(self, message):
        """
        Function that gets the body of the mail and decodes it in base 64.
        Returns the formatted body.
        """
        body = message["payload"]["parts"][0]["body"]["data"]
        body = body.replace("-", "+").replace("_", "/")
        decoded_body = base64.b64decode(body)
        formatted_body = str(decoded_body).lower()
        return formatted_body

    def get_headers(self, message):
        """
        Function that gets the headers of the mail and store the data in a new dictionary.
        Returns the new dictionary.
        """
        new_headers = {}
        headers = message["payload"]["headers"]
        for header in headers:
            if header["name"] == "Date":
                new_headers["date_email"] = header["value"]
            if header["name"] == "From":
                new_headers["from_email"] = header["value"]
            if header["name"] == "Subject":
                new_headers["subject_email"] = header["value"]
        return new_headers


class Gmail:
    """
    This class object allows the connection with the Gmail API to get the information of the mails.
    """

    def __init__(self):
        """
        Function that defines the read-only scope of mail and provides the credentials to access the mail account.
        """
        self.scopes = ["https://www.googleapis.com/auth/gmail.readonly"]

        credentials = self._validate_credentials()
        self.service = build("gmail", "v1", credentials=credentials)

    def _validate_credentials(self):
        """
        Function that validates the existence of the credentials.
        If they are not valid, it refreshes the token to guarantee the connection.
        Returns the access credentials.
        """
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
        """
        Function that gets all the mails associated with the Gmail account. Returns a list of messages.
        """
        result = self.service.users().messages().list(userId="me").execute()
        return result["messages"]

    def get_message(self, message):
        """
        Function that gets the specific mail to read. Returns a variable with the mail.
        """
        result = (
            self.service.users().messages().get(userId="me", id=message["id"]).execute()
        )
        return result


def main():
    """
    The main function calls the Gmail and Mail classes to get the headers and body of the mails.
    Look for the word "DevOps" within the bodies, and for those emails that meet the condition,
    the date, sender, and subject are stored in the database.
    Prints a message with the number of emails that meet the condition, the total and the number of records stored in the database.
    """
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
