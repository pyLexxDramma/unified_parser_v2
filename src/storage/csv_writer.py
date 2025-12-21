from __future__ import annotations
import csv
import logging
import os
from typing import Any, Dict
import datetime

from src.storage.file_writer import FileWriter, FileWriterOptions
from src.config.settings import Settings

logger = logging.getLogger(__name__)


class DateTimeJSONEncoder:
    """Кастомный encoder для JSON, который конвертирует datetime объекты в строки ISO формата"""
    @staticmethod
    def default(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

class CSVWriter(FileWriter):
    def __init__(self, settings: Settings):
        if hasattr(settings, 'app_config'):
            writer_opts = settings.app_config.writer
        else:
            writer_opts = settings.writer

        file_writer_options = FileWriterOptions(
            encoding=writer_opts.encoding,
            verbose=writer_opts.verbose,
            format=writer_opts.format,
            output_dir=writer_opts.output_dir
        )
        super().__init__(options=file_writer_options)
        self.fieldnames: list = None
        self.header_written: bool = False
        self.file_handle = None
        self.writer = None

    def open(self):
        if not self.file_path:
            raise ValueError("File path is not set. Use set_file_path() first.")

        output_dir = os.path.dirname(self.file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")

        try:
            self.file_handle = open(self.file_path, 'w', newline='', encoding=self.options.encoding)
            self.writer = csv.writer(self.file_handle)
            logger.info(f"CSV file opened: {self.file_path}")
        except Exception as e:
            logger.error(f"Error opening CSV file {self.file_path}: {e}", exc_info=True)
            raise

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            logger.info(f"CSV file closed. Wrote {self.wrote_count} records.")

    def write(self, data: Dict[str, Any]):
        if not self.writer:
            logger.error("CSV writer not initialized. Call open() first.")
            return

        if self.fieldnames is None:
            self.fieldnames = list(data.keys())
            if not self.header_written:
                self.writer.writerow(self.fieldnames)
                self.header_written = True

        # Обрабатываем данные для правильной кодировки
        row = []
        for field in self.fieldnames:
            value = data.get(field)
            # Проверяем None в начале
            if value is None:
                value = ''
            # Если значение - строка, убеждаемся что она в правильной кодировке
            elif isinstance(value, str):
                # Убираем недопустимые символы и нормализуем
                try:
                    # Проверяем, что строка правильно закодирована в UTF-8
                    value.encode('utf-8')
                    # Убираем BOM и другие невидимые символы
                    value = value.replace('\ufeff', '').replace('\u200b', '').strip()
                except UnicodeEncodeError:
                    # Если есть проблемы, пытаемся исправить
                    value = value.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
                except Exception as e:
                    logger.warning(f"Error processing string value for field {field}: {e}")
                    value = str(value).encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
            elif isinstance(value, (list, dict)):
                # Для сложных структур используем JSON с правильной кодировкой
                import json
                try:
                    # Для списков отзывов логируем количество элементов
                    if field == 'detailed_reviews' and isinstance(value, list):
                        logger.debug(f"Serializing {len(value)} reviews to JSON for field '{field}'")
                    # Используем кастомный encoder для обработки datetime объектов
                    value = json.dumps(value, ensure_ascii=False, default=DateTimeJSONEncoder.default)
                    # Проверяем размер JSON-строки (CSV может иметь ограничения)
                    if len(value) > 1000000:  # Если больше 1MB, логируем предупреждение
                        logger.warning(f"Large JSON value for field '{field}': {len(value)} characters")
                except Exception as e:
                    logger.error(f"Error serializing {type(value)} for field {field}: {e}", exc_info=True)
                    # В случае ошибки пытаемся сохранить хотя бы количество элементов
                    if isinstance(value, list):
                        value = f"[Error serializing {len(value)} items: {str(e)}]"
            else:
                # Для других типов конвертируем в строку
                try:
                    value = str(value)
                    value.encode('utf-8')
                except:
                    value = str(value).encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
            row.append(value)
        
        self.writer.writerow(row)
        self.wrote_count += 1

