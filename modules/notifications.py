import os
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from typing import List
from config.config import logger
"""
Модуль уведомлений с реализацией паттерна "Наблюдатель" для ETL-системы.

Позволяет отправлять уведомления о событиях по email и в Telegram.
Обеспечивает расширяемость за счёт интерфейса Observer и субъекта NotificationSubject.

Классы
------
Observer
    Абстрактный интерфейс наблюдателя для уведомлений.
EmailObserver
    Реализация отправки email-уведомлений с поддержкой вложений.
TelegramObserver
    Реализация отправки уведомлений в Telegram.
NotificationSubject
    Субъект для управления подписчиками и рассылки уведомлений всем наблюдателям.

Пример использования
--------------------
subject = NotificationSubject()s
subject.attach(EmailObserver())
subject.attach(TelegramObserver())
subject.notify("ETL завершён", subject="Результаты", attachment_path="file.csv")
"""


# --- Интерфейс наблюдателя ---
class Observer:
    def update(self, message: str, **kwargs):
        raise NotImplementedError


# --- Реализация Email-уведомления ---
class EmailObserver(Observer):
    def __init__(self):
        self.email_config = {
            'smtp_server': os.getenv('EMAIL_SMTP_SERVER'),
            'smtp_port': int(os.getenv('EMAIL_SMTP_PORT', 465)),
            'user': os.getenv('EMAIL_USER'),
            'password': os.getenv('EMAIL_PASSWORD'),
            'sender': os.getenv('EMAIL_SENDER'),
            'recipient': os.getenv('EMAIL_RECIPIENT')
        }

    def update(self, message: str, **kwargs):
        subject = kwargs.get("subject", "Уведомление")
        attachment_path = kwargs.get("attachment_path")
        if not all(self.email_config.values()):
            logger.warning("Попытка отправки email без полной конфигурации")
            return False
        try:
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = self.email_config['sender']
            msg['To'] = self.email_config['recipient']
            msg.attach(MIMEText(message, "plain"))
            if attachment_path:
                self._attach_file(msg, attachment_path)
            with smtplib.SMTP_SSL(self.email_config['smtp_server'],
                                  self.email_config['smtp_port']) as smtp:
                smtp.login(self.email_config['user'], self.email_config['password'])
                smtp.send_message(msg)
            logger.info(f"Email отправлен: {subject}")
            return True
        except Exception as e:
            logger.warning(f"Ошибка отправки email: {str(e)}")
            return False

    def _attach_file(self, msg: MIMEMultipart, file_path: str) -> None:
        try:
            with open(file_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition",
                                f"attachment; filename={os.path.basename(file_path)}")
                msg.attach(part)
        except Exception as e:
            logger.warning(f"Ошибка прикрепления файла: {str(e)}")
            raise


# --- Реализация Telegram-уведомления ---
class TelegramObserver(Observer):
    def __init__(self):
        self.tg_config = {
            'token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'chat_id': os.getenv('TELEGRAM_CHAT_ID')
        }

    def update(self, message: str, **kwargs):
        if not all(self.tg_config.values()):
            logger.warning("Попытка отправки в Telegram без полной конфигурации")
            return False
        try:
            url = f'https://api.telegram.org/bot{self.tg_config["token"]}/sendMessage'
            response = requests.post(
                url,
                json={'chat_id': self.tg_config["chat_id"], 'text': message},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"Telegram сообщение отправлено: {message[:50]}...")
            return True
        except Exception as e:
            logger.warning(f"Ошибка отправки в Telegram: {str(e)}")
            return False


# --- Субъект (Subject) для управления подписчиками ---
class NotificationSubject:
    def __init__(self):
        self._observers: List[Observer] = []  # type: ignore

    def attach(self, observer: Observer):
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: Observer):
        if observer in self._observers:
            self._observers.remove(observer)

    def notify(self, message: str, **kwargs):
        for observer in self._observers:
            observer.update(message, **kwargs)
