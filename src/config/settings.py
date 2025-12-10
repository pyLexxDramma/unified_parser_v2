from __future__ import annotations
import json
import logging
import os
import pathlib
from typing import Dict, Any, Optional
import psutil
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)

def get_project_root() -> pathlib.Path:
    current_path = pathlib.Path(__file__).resolve()
    for _ in range(5):
        if (current_path / '.git').exists() or (current_path / 'config.json').exists() or (current_path / '.env').exists():
            return current_path
        current_path = current_path.parent
    fallback_root = pathlib.Path(os.getcwd())
    logger.warning(f"Project root markers not found. Falling back to current working directory: {fallback_root}")
    return fallback_root

class ProxySettings(BaseModel):
    enabled: bool = False
    server: str = ""
    port: int = 8080
    username: str = ""
    password: str = ""
    type: str = "http"

class ChromeSettings(BaseModel):
    headless: bool = False
    chromedriver_path: str = ""
    silent_browser: bool = True
    binary_path: Optional[pathlib.Path] = None
    start_maximized: bool = False
    disable_images: bool = True
    memory_limit: int = Field(default_factory=lambda: int(psutil.virtual_memory().total / 1024 ** 2 * 0.75) if psutil else 1024)
    proxy_server: Optional[str] = None

class ParserOptions(BaseModel):
    retries: int = 3
    timeout: float = 10.0
    skip_404_response: bool = True
    delay_between_clicks: int = 0
    max_records: int = Field(default_factory=lambda: int(psutil.virtual_memory().total / 1024 ** 2 * 0.75) // 2 if psutil else 1000)
    use_gc: bool = False
    gc_pages_interval: int = 10
    yandex_captcha_wait: int = 20
    yandex_reviews_scroll_step: int = 500
    yandex_reviews_scroll_max_iter: int = 100
    yandex_reviews_scroll_min_iter: int = 30
    yandex_card_selectors: list[str] = Field(default_factory=lambda: ["div.search-business-snippet-view", "div.search-snippet-view__body._type_business", "div[class*='search-snippet-view__body'][class*='_type_business']", "a[href*='/maps/org/']:not([href*='/gallery/'])"])
    yandex_scroll_container: str = ".scroll__container, .scroll__content, .search-list-view__list"
    yandex_scrollable_element_selector: str = ".scroll__container, .scroll__content, [class*='search-list-view'], [class*='scroll']"
    yandex_scroll_step: int = 800
    yandex_scroll_max_iter: int = 200
    yandex_scroll_wait_time: float = 2.0
    yandex_min_cards_threshold: int = 500
    gis_scroll_step: int = 500
    gis_scroll_max_iter: int = 100
    gis_scroll_wait_time: float = 0.5
    gis_reviews_scroll_step: int = 500
    gis_reviews_scroll_max_iter: int = 100
    gis_reviews_scroll_min_iter: int = 30
    gis_card_selectors: list[str] = Field(default_factory=lambda: ["a[href*='/firm/']", "a[href*='/station/']"])
    gis_scroll_container: str = "[class*='_1rkbbi0x'], [class*='scroll'], [class*='list'], [class*='results']"

class WriterOptions(BaseModel):
    encoding: str = 'utf-8-sig'
    verbose: bool = True
    format: str = "csv"
    output_dir: str = "./output"

class LogOptions(BaseModel):
    gui_format: str = '%(asctime)s.%(msecs)03d | %(message)s'
    cli_format: str = '%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s'
    gui_datefmt: str = '%H:%M:%S'
    cli_datefmt: str = '%d/%m/%Y %H:%M:%S'
    level: str = 'INFO'
    @validator('level')
    def level_validation(cls, v: str) -> str:
        v = v.upper()
        allowed_levels = ('ERROR', 'WARNING', 'WARN', 'INFO', 'DEBUG', 'FATAL', 'CRITICAL', 'NOTSET')
        if v not in allowed_levels:
            raise ValueError(f'Invalid log level: {v}. Must be one of {allowed_levels}')
        return v

class AppConfig(BaseModel):
    app_name: str = "Unified Parser"
    project_root: str = Field(default_factory=lambda: str(get_project_root()))
    config_file: str = Field(default_factory=lambda: str(get_project_root() / "config.json"))
    env_file: str = Field(default_factory=lambda: str(get_project_root() / ".env"))
    root_directory: str = "/"
    environment: str = "development"
    log_level: str = "info"
    password: Optional[str] = None
    chrome: ChromeSettings = Field(default_factory=ChromeSettings)
    writer: WriterOptions = Field(default_factory=WriterOptions)

class Settings(BaseModel):
    chrome: ChromeSettings = Field(default_factory=ChromeSettings)
    parser: ParserOptions = Field(default_factory=ParserOptions)
    log: LogOptions = Field(default_factory=LogOptions)
    app_config: AppConfig = Field(default_factory=AppConfig)
    proxy: ProxySettings = Field(default_factory=ProxySettings)
    project_root: str = "."
    config_file: Optional[str] = None
    env_file: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        env_file_path = pathlib.Path(self.env_file or (get_project_root() / ".env"))
        if env_file_path.exists():
            try:
                load_dotenv(dotenv_path=env_file_path)
                logger.info(f"Loaded environment variables from: {env_file_path}")

                if os.getenv('PROXY_ENABLED'):
                    self.proxy.enabled = os.getenv('PROXY_ENABLED', 'false').lower() in ('true', '1', 'yes')
                if os.getenv('PROXY_SERVER'):
                    self.proxy.server = os.getenv('PROXY_SERVER', '')
                if os.getenv('PROXY_PORT'):
                    try:
                        self.proxy.port = int(os.getenv('PROXY_PORT', '8080'))
                    except ValueError:
                        pass
                if os.getenv('PROXY_USERNAME'):
                    self.proxy.username = os.getenv('PROXY_USERNAME', '')
                if os.getenv('PROXY_PASSWORD'):
                    self.proxy.password = os.getenv('PROXY_PASSWORD', '')
                if os.getenv('PROXY_TYPE'):
                    self.proxy.type = os.getenv('PROXY_TYPE', 'http')

                if os.getenv('SITE_PASSWORD'):
                    self.app_config.password = os.getenv('SITE_PASSWORD')

                if os.getenv('SMTP_SERVER'):
                    if not hasattr(self, 'email_settings'):
                        from pydantic import BaseModel
                        class EmailSettings(BaseModel):
                            smtp_server: str = ""
                            smtp_port: int = 587
                            smtp_user: str = ""
                            smtp_password: str = ""
                        self.email_settings = EmailSettings()
                    self.email_settings.smtp_server = os.getenv('SMTP_SERVER', '')
                    if os.getenv('SMTP_PORT'):
                        try:
                            self.email_settings.smtp_port = int(os.getenv('SMTP_PORT', '587'))
                        except ValueError:
                            pass
                    if os.getenv('SMTP_USER'):
                        self.email_settings.smtp_user = os.getenv('SMTP_USER', '')
                    if os.getenv('SMTP_PASSWORD'):
                        self.email_settings.smtp_password = os.getenv('SMTP_PASSWORD', '')
            except Exception as e:
                logger.warning(f"Could not load .env file from {env_file_path}: {e}")
        config_file_path = pathlib.Path(self.config_file or (get_project_root() / "config.json"))
        config_data = {}
        if config_file_path.exists():
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    logger.info(f"Loaded configuration from: {config_file_path}")
                    if 'parser' in config_data:
                        parser_data = config_data['parser']
                        for key, value in parser_data.items():
                            if hasattr(self.parser, key):
                                setattr(self.parser, key, value)
                    if 'chrome' in config_data:
                        chrome_data = config_data['chrome']
                        for key, value in chrome_data.items():
                            if hasattr(self.chrome, key):
                                setattr(self.chrome, key, value)
                    elif 'chrome' in config_data.get('app', {}):
                        chrome_data = config_data['app'].get('chrome', {})
                        for key, value in chrome_data.items():
                            if hasattr(self.chrome, key):
                                setattr(self.chrome, key, value)
                    if 'app' in config_data:
                        app_data = config_data['app']
                        if 'password' in app_data and not os.getenv('SITE_PASSWORD'):
                            self.app_config.password = app_data['password']
                    if 'proxy' in config_data:
                        proxy_data = config_data['proxy']
                        for key, value in proxy_data.items():
                            if hasattr(self.proxy, key) and not os.getenv(f'PROXY_{key.upper()}'):
                                setattr(self.proxy, key, value)
                    if 'email' in config_data:
                        email_data = config_data['email']
                        if not hasattr(self, 'email_settings'):
                            from pydantic import BaseModel
                            class EmailSettings(BaseModel):
                                smtp_server: str = ""
                                smtp_port: int = 587
                                smtp_user: str = ""
                                smtp_password: str = ""
                            self.email_settings = EmailSettings()
                        for key, value in email_data.items():
                            if hasattr(self.email_settings, key) and not os.getenv(f'SMTP_{key.upper()}'):
                                setattr(self.email_settings, key, value)
            except Exception as e:
                logger.warning(f"Could not load config.json from {config_file_path}: {e}")

try:
    settings = Settings()
    log_level_str = settings.log.level.upper()
    log_level_int = getattr(logging, log_level_str) if log_level_str in logging._nameToLevel else logging.INFO
    from logging.handlers import RotatingFileHandler
    import sys
    log_dir = os.path.join(settings.project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "parser.log")
    log_format = settings.log.cli_format
    date_format = settings.log.cli_datefmt
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level_int)
    root_logger.handlers.clear()
    import re
    
    class FlushingStreamHandler(logging.StreamHandler):
        # Регулярное выражение для удаления ANSI escape-кодов
        ANSI_ESCAPE_RE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
        
        def __init__(self, stream=None):
            super().__init__(stream)
            # Убеждаемся, что поток настроен правильно
            if stream and hasattr(stream, 'reconfigure'):
                try:
                    stream.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
                except:
                    pass
        
        def emit(self, record):
            try:
                # Форматируем сообщение
                msg = self.format(record)
                # Удаляем все ANSI escape-коды для совместимости с Windows PowerShell
                msg = self.ANSI_ESCAPE_RE.sub('', msg)
                # Записываем в поток
                stream = self.stream
                stream.write(msg + self.terminator)
                # Принудительно сбрасываем буфер после каждого сообщения
                self.flush()
            except Exception:
                self.handleError(record)
    
    console_handler = FlushingStreamHandler(sys.stdout)
    console_handler.setLevel(log_level_int)
    console_formatter = logging.Formatter(log_format, datefmt=date_format)
    console_handler.setFormatter(console_formatter)
    
    # Настраиваем буферизацию для немедленного вывода
    if hasattr(console_handler.stream, 'reconfigure'):
        try:
            console_handler.stream.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
        except:
            pass
    
    # Убеждаемся, что stderr тоже не буферизуется
    if hasattr(sys.stderr, 'reconfigure'):
        try:
            sys.stderr.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')
        except:
            pass
    
    root_logger.addHandler(console_handler)
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    file_handler.setLevel(log_level_int)
    file_formatter = logging.Formatter(log_format, datefmt=date_format)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    logger.setLevel(log_level_int)
    for logger_name in ['src.parsers', 'src.parsers.yandex_parser', 'src.parsers.gis_parser', 'src.drivers', 'src.drivers.selenium_driver', 'src.webapp', 'src.webapp.app']:
        module_logger = logging.getLogger(logger_name)
        module_logger.setLevel(log_level_int)
        module_logger.propagate = True
    logger.info(f"Logger configured with level: {log_level_str}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Settings loaded successfully.")
except Exception as e:
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
    logging.error(f"FATAL: Failed to initialize settings or logger: {e}", exc_info=True)

