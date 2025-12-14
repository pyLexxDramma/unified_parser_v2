from __future__ import annotations
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any
from src.config.settings import Settings

logger = logging.getLogger(__name__)


def send_parsing_completion_email(
    email: str,
    task_id: str,
    status: str,
    company_name: str,
    settings: Settings,
    error: Optional[str] = None,
    cards_count: int = 0
) -> bool:
    """
    Отправляет email уведомление о завершении парсинга.
    
    Args:
        email: Email адрес получателя
        task_id: ID задачи парсинга
        status: Статус задачи (COMPLETED, FAILED)
        company_name: Название компании
        settings: Настройки приложения (содержит email_settings)
        error: Текст ошибки (если статус FAILED)
        cards_count: Количество найденных карточек
    
    Returns:
        True если email отправлен успешно, False в противном случае
    """
    try:
        # Проверяем наличие настроек SMTP
        if not hasattr(settings, 'email_settings') or not settings.email_settings:
            logger.warning("Email settings not configured, skipping email notification")
            logger.info(f"Email notification would be sent to: {email} (status: {status}, company: {company_name})")
            return False
        
        email_settings = settings.email_settings
        if not email_settings.smtp_server or not email_settings.smtp_user:
            logger.warning("SMTP server or user not configured, skipping email notification")
            logger.info(f"Email notification would be sent to: {email} (status: {status}, company: {company_name})")
            return False
        
        if not email_settings.smtp_password:
            logger.warning("SMTP password not configured, skipping email notification")
            logger.info(f"Email notification would be sent to: {email} (status: {status}, company: {company_name})")
            logger.info("To enable email notifications, configure SMTP settings in .env or config.json")
            return False
        
        # Формируем сообщение
        msg = MIMEMultipart()
        msg['From'] = email_settings.smtp_user
        msg['To'] = email
        msg['Subject'] = f"Парсинг завершен: {company_name}"
        
        # Тело письма
        if status == "COMPLETED":
            body = f"""
Парсинг успешно завершен.

Компания: {company_name}
Задача: {task_id}
Найдено карточек: {cards_count}

Вы можете просмотреть результаты в веб-интерфейсе парсера.
"""
        else:
            body = f"""
Парсинг завершен с ошибкой.

Компания: {company_name}
Задача: {task_id}
Ошибка: {error or 'Неизвестная ошибка'}

Пожалуйста, проверьте логи для получения дополнительной информации.
"""
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Отправляем email
        try:
            if email_settings.smtp_port == 465:
                # SSL соединение
                server = smtplib.SMTP_SSL(email_settings.smtp_server, email_settings.smtp_port)
            else:
                # TLS соединение
                server = smtplib.SMTP(email_settings.smtp_server, email_settings.smtp_port)
                server.starttls()
            
            if email_settings.smtp_password:
                server.login(email_settings.smtp_user, email_settings.smtp_password)
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent successfully to {email} for task {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {email}: {e}", exc_info=True)
            return False
            
    except Exception as e:
        logger.error(f"Error preparing email notification: {e}", exc_info=True)
        return False

