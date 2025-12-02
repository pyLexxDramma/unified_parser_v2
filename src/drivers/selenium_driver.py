from __future__ import annotations
import logging
import os
import tempfile
import threading
import sys
import time
from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

from selenium.webdriver import Chrome, ChromeOptions as SeleniumChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.remote.webelement import WebElement

from src.drivers.base_driver import BaseDriver
from src.config.settings import Settings

logger = logging.getLogger(__name__)

def extract_credentials_from_proxy_url(proxy_url: str) -> tuple:
    parsed_url = urlparse(proxy_url)
    if '@' in parsed_url.netloc:
        credentials = parsed_url.netloc.split('@')[0]
        if ':' in credentials:
            username, password = credentials.split(':', 1)
            return username, password
    return None, None

def create_proxy_auth_extension(proxy_host: str, proxy_port: int, username: str, password: str) -> str:
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
              singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
              },
              bypassList: ["localhost"]
            }
          };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (proxy_host, proxy_port, username, password)

    temp_dir = tempfile.mkdtemp()
    extension_dir = os.path.join(temp_dir, "proxy_auth_extension")
    os.makedirs(extension_dir, exist_ok=True)
    
    manifest_path = os.path.join(extension_dir, "manifest.json")
    background_path = os.path.join(extension_dir, "background.js")
    
    with open(manifest_path, 'w', encoding='utf-8') as f:
        f.write(manifest_json)
    
    with open(background_path, 'w', encoding='utf-8') as f:
        f.write(background_js)
    
    logger.info(f"Proxy auth extension created at: {extension_dir}")
    return extension_dir

class SeleniumTab:
    def __init__(self, driver: "SeleniumDriver"):
        self._driver = driver
        self._default_timeout = 10

    def set_default_timeout(self, timeout: int):
        self._default_timeout = timeout

    def wait_for_element(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> Optional[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            if not self._driver or not self._driver.driver:
                return None
            wait = WebDriverWait(self._driver.driver, wait_timeout)
            return wait.until(EC.presence_of_element_located(locator))
        except TimeoutException:
            return None

    def wait_for_response(self, url_pattern: str, timeout: int = 10) -> Optional[str]:
        return self._driver.wait_response(url_pattern, timeout)

class SeleniumDriver(BaseDriver):
    def __init__(self, settings: Settings, proxy: Optional[str] = None):
        self.settings = settings
        self.proxy = proxy
        self.driver: Optional[Chrome] = None
        self._tab: Optional[SeleniumTab] = None
        self._is_running = False
        self.current_url: Optional[str] = None

        self._tab = SeleniumTab(self)

    def _initialize_driver(self):
        logger.info("_initialize_driver() called")
        if self.driver is not None:
            logger.info("Driver already exists, returning")
            return

        logger.info("Creating ChromeOptions...")
        options = SeleniumChromeOptions()
        
        # Настройка прокси
        logger.info(f"Proxy settings check: enabled={self.settings.proxy.enabled}, server={self.settings.proxy.server}, proxy param={self.proxy}")
        
        if self.settings.proxy.enabled and self.settings.proxy.server:
            proxy_url = f"{self.settings.proxy.server}:{self.settings.proxy.port}"
            username = self.settings.proxy.username or ""
            password = self.settings.proxy.password or ""
            
            if self.proxy:
                username, password = extract_credentials_from_proxy_url(self.proxy) or (username, password)
                proxy_url = self.proxy.split('@')[-1] if '@' in self.proxy else proxy_url
            
            if username and password:
                proxy_host = proxy_url.split(':')[0] if ':' in proxy_url else self.settings.proxy.server
                proxy_port = int(proxy_url.split(':')[1]) if ':' in proxy_url else self.settings.proxy.port
                proxy_extension_dir = create_proxy_auth_extension(proxy_host, proxy_port, username, password)
                options.add_argument(f"--load-extension={proxy_extension_dir}")
                logger.info(f"Proxy auth extension loaded from: {proxy_extension_dir}")
            else:
                options.add_argument(f'--proxy-server={proxy_url}')
                logger.info(f"Proxy server configured: {proxy_url}")
        else:
            options.add_argument("--no-proxy-server")
            options.add_argument("--proxy-bypass-list=*")
            logger.info("Proxy DISABLED - running without proxy")

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        # Обновленный User-Agent для лучшей защиты от капчи
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        
        # Дополнительные настройки для обхода детектирования
        options.add_argument("--lang=ru-RU,ru")
        options.add_argument("--accept-lang=ru-RU,ru")
        options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.images": 1,
            "intl.accept_languages": "ru-RU,ru"
        })

        # Принудительно включаем headless режим для фоновой работы
        headless_enabled = getattr(self.settings.chrome, 'headless', False)
        logger.info(f"Chrome headless setting from config: {headless_enabled}")
        # Если headless не включен в конфиге, принудительно включаем для фоновой работы
        if not headless_enabled:
            logger.warning("Headless mode was False in config, but forcing it to True for background operation")
            headless_enabled = True
        
        if headless_enabled:
            options.add_argument("--headless=new")  # Новый headless режим Chrome
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-software-rasterizer")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-renderer-backgrounding")
            options.add_argument("--window-size=1920,1080")
            # Отключаем звук и другие функции, которые могут мешать
            options.add_argument("--mute-audio")
            options.add_argument("--disable-notifications")
            logger.info("Headless mode enabled - browser will run in background")

        chromedriver_path = None
        import glob
        
        # Приоритет 0: переменная окружения CHROMEDRIVER_PATH (удобно для прод-сервера)
        env_chromedriver = os.getenv("CHROMEDRIVER_PATH")
        if env_chromedriver and os.path.exists(env_chromedriver):
            chromedriver_path = env_chromedriver
            logger.info(f"Using ChromeDriver from CHROMEDRIVER_PATH: {chromedriver_path}")
        
        # Приоритет 1: используем путь из config.json (если указан и существует)
        if not chromedriver_path and self.settings.chrome.chromedriver_path and os.path.exists(self.settings.chrome.chromedriver_path):
            chromedriver_path = self.settings.chrome.chromedriver_path
            logger.info(f"Using ChromeDriver from config: {chromedriver_path}")
        
        # Приоритет 2: пробуем найти ChromeDriver в .wdm (где ChromeDriverManager сохраняет драйверы)
        if not chromedriver_path:
            wdm_path = os.path.join(os.path.expanduser("~"), ".wdm", "drivers", "chromedriver")
            if os.path.exists(wdm_path):
                matches = glob.glob(os.path.join(wdm_path, "**", "chromedriver.exe"), recursive=True)
                if matches:
                    # Берем самый новый (последний по времени модификации)
                    chromedriver_path = max(matches, key=os.path.getmtime)
                    logger.info(f"Found ChromeDriver in .wdm: {chromedriver_path}")
        
        # Приоритет 3: используем ChromeDriverManager для автоматической установки подходящей версии
        if not chromedriver_path:
            logger.info("ChromeDriver not found in .wdm or config. Using ChromeDriverManager to auto-install...")
            logger.info("====== WebDriver manager ======")
            try:
                # Используем threading для таймаута ChromeDriverManager
                manager_result = [None]
                manager_error = [None]
                
                def install_driver():
                    try:
                        logger.info("Thread: Starting ChromeDriverManager().install()...")
                        manager_result[0] = ChromeDriverManager().install()
                        logger.info(f"Thread: ChromeDriverManager().install() completed: {manager_result[0]}")
                    except Exception as e:
                        logger.error(f"Thread: ChromeDriverManager().install() failed: {e}", exc_info=True)
                        manager_error[0] = e
                
                logger.info("Creating thread for ChromeDriverManager installation...")
                thread = threading.Thread(target=install_driver, daemon=True)
                thread.start()
                logger.info("Thread started, waiting for ChromeDriverManager installation (max 30 seconds)...")
                thread.join(timeout=30)  # Таймаут 30 секунд
                logger.info(f"Thread join completed. Thread alive: {thread.is_alive()}")
                
                if thread.is_alive():
                    logger.error("ChromeDriverManager installation timeout (30 seconds)")
                    raise TimeoutError("Таймаут при установке ChromeDriver (30 секунд). Проверьте интернет-соединение.")
                
                if manager_error[0]:
                    raise manager_error[0]
                
                if manager_result[0]:
                    chromedriver_path = manager_result[0]
                    logger.info(f"ChromeDriverManager installed ChromeDriver at: {chromedriver_path}")
                else:
                    raise Exception("ChromeDriverManager не вернул путь к драйверу")
            except TimeoutError:
                raise
            except Exception as e:
                logger.error(f"Failed to install ChromeDriver: {e}", exc_info=True)
                raise Exception(f"Не удалось установить ChromeDriver. Ошибка: {e}")
        
        if not chromedriver_path or not os.path.exists(chromedriver_path):
            raise Exception(f"ChromeDriver не найден по пути: {chromedriver_path}")
        
        # Настраиваем Service с таймаутами
        service = Service(chromedriver_path)
        # Увеличиваем таймауты для Service
        service.service_args = []
        
        logger.info("Creating Chrome WebDriver instance...")
        try:
            # Создаем драйвер с таймаутом через threading (используем глобальный импорт)
            logger.info(f"Attempting to create Chrome driver with path: {chromedriver_path}")
            
            driver_created = threading.Event()
            driver_error = [None]
            
            def create_driver():
                try:
                    logger.info("Thread: Starting Chrome() call...")
                    logger.info(f"Thread: Chrome options: headless={self.settings.chrome.headless}")
                    logger.info(f"Thread: Chrome service path: {chromedriver_path}")
                    logger.info(f"Thread: Options arguments count: {len(options.arguments)}")
                    logger.info(f"Thread: Service executable path: {service.service.executable_path if hasattr(service, 'service') and hasattr(service.service, 'executable_path') else 'N/A'}")
                    # Пытаемся создать драйвер с максимальной защитой от завершения процесса
                    logger.info("Thread: Calling Chrome(service=service, options=options)...")
                    import time as time_module
                    start_time = time_module.time()
                    try:
                        # Обертываем в try-except для перехвата любых исключений
                        import sys
                        import traceback
                        import os
                        # Сохраняем текущий обработчик SystemExit
                        original_exit = sys.exit
                        original_excepthook = sys.excepthook
                        
                        def safe_exit(code=0):
                            """Безопасный exit, который не завершает процесс"""
                            logger.warning(f"Thread: sys.exit({code}) was called, but we're preventing process termination")
                            raise Exception(f"Chrome attempted to exit with code {code}")
                        
                        def safe_excepthook(exc_type, exc_value, exc_traceback):
                            """Безопасный обработчик исключений"""
                            logger.error(f"Thread: Unhandled exception in Chrome thread: {exc_type.__name__}: {exc_value}")
                            logger.error(f"Thread: Exception traceback: {traceback.format_exception(exc_type, exc_value, exc_traceback)}")
                            # Не вызываем оригинальный excepthook, чтобы не завершать процесс
                        
                        # Временно заменяем sys.exit и sys.excepthook
                        sys.exit = safe_exit
                        sys.excepthook = safe_excepthook
                        
                        try:
                            self.driver = Chrome(service=service, options=options)
                            elapsed = time_module.time() - start_time
                            logger.info(f"Thread: Chrome() call completed successfully in {elapsed:.2f} seconds")
                        except SystemExit as se:
                            # Перехватываем SystemExit, чтобы не завершать процесс
                            elapsed = time_module.time() - start_time
                            logger.error(f"Thread: Chrome() call triggered SystemExit after {elapsed:.2f} seconds: {se}")
                            logger.error(f"Thread: SystemExit traceback: {traceback.format_exc()}")
                            raise Exception(f"Chrome вызвал SystemExit: {se}")
                        except BaseException as be:
                            # Перехватываем все базовые исключения, включая KeyboardInterrupt
                            elapsed = time_module.time() - start_time
                            logger.error(f"Thread: Chrome() call raised BaseException after {elapsed:.2f} seconds: {be}")
                            logger.error(f"Thread: BaseException traceback: {traceback.format_exc()}")
                            raise Exception(f"Chrome вызвал критическое исключение: {be}")
                        finally:
                            # Восстанавливаем оригинальные обработчики
                            sys.exit = original_exit
                            sys.excepthook = original_excepthook
                    except Exception as chrome_error:
                        elapsed = time_module.time() - start_time
                        logger.error(f"Thread: Chrome() call failed after {elapsed:.2f} seconds: {chrome_error}", exc_info=True)
                        raise
                    driver_created.set()
                except Exception as e:
                    logger.error(f"Thread: Chrome() call failed: {e}", exc_info=True)
                    driver_error[0] = e
                    driver_created.set()
                except BaseException as be:
                    # Перехватываем все базовые исключения в потоке
                    logger.error(f"Thread: Chrome() call raised BaseException: {be}", exc_info=True)
                    driver_error[0] = Exception(f"Критическая ошибка при создании Chrome: {be}")
                    driver_created.set()
            
            thread = threading.Thread(target=create_driver, daemon=True)
            logger.info("About to start thread for Chrome driver creation...")
            thread.start()
            logger.info("Thread started, waiting for Chrome driver creation (max 30 seconds)...")
            
            # Ждем создания драйвера с таймаутом 30 секунд
            # Проверяем каждые 2 секунды, чтобы видеть прогресс
            waited_time = 0
            check_interval = 2
            max_wait_time = 30
            logger.info(f"Entering wait loop, max wait time: {max_wait_time} seconds")
            while waited_time < max_wait_time:
                if driver_created.wait(timeout=check_interval):
                    logger.info(f"Driver creation event received after {waited_time} seconds")
                    break
                waited_time += check_interval
                thread_alive = thread.is_alive()
                logger.info(f"Still waiting for Chrome driver creation... ({waited_time}/{max_wait_time} seconds, thread alive: {thread_alive})")
                if not thread_alive:
                    logger.warning("Thread is not alive, but driver_created event is not set. This might indicate an issue.")
                    # Если поток завершился, но событие не установлено, значит была ошибка
                    if driver_error[0]:
                        logger.error(f"Driver creation failed in thread: {driver_error[0]}")
                        raise driver_error[0]
                    break
            
            if not driver_created.is_set():
                logger.error(f"Timeout creating Chrome WebDriver ({max_wait_time} seconds) - Chrome() call did not complete")
                logger.error(f"Thread alive status: {thread.is_alive()}, driver_error: {driver_error[0]}")
                # Пытаемся убить зависший процесс Chrome и chromedriver
                try:
                    import subprocess
                    import platform
                    if platform.system() == 'Windows':
                        # Убиваем все процессы chrome и chromedriver на Windows
                        logger.info("Attempting to kill stuck Chrome and chromedriver processes...")
                        subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], capture_output=True, timeout=5)
                        subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], capture_output=True, timeout=5)
                        logger.info("Killed stuck Chrome and chromedriver processes using taskkill")
                    else:
                        # Для Linux/Mac используем psutil
                        chrome_processes = [p for p in __import__('psutil').process_iter(['pid', 'name']) if 'chrome' in p.info['name'].lower() or 'chromedriver' in p.info['name'].lower()]
                        for proc in chrome_processes:
                            try:
                                proc.kill()
                                logger.info(f"Killed stuck process: {proc.info['pid']} ({proc.info['name']})")
                            except:
                                pass
                except Exception as kill_error:
                    logger.warning(f"Error killing Chrome processes: {kill_error}")
                raise TimeoutError(f"Таймаут при создании Chrome WebDriver ({max_wait_time} секунд). Chrome не отвечает. Проверьте, что Chrome установлен и доступен.")
            
            if driver_error[0]:
                raise driver_error[0]
            
            if self.driver is None:
                raise Exception("Драйвер не был создан")
            
            logger.info("Chrome WebDriver created successfully")
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self._is_running = True
            logger.info("Driver initialized and started successfully")
        except TimeoutError as e:
            logger.error(f"Timeout creating Chrome WebDriver: {e}", exc_info=True)
            self._is_running = False
            raise Exception(f"Таймаут при создании Chrome WebDriver. Chrome не отвечает. Проверьте, что Chrome установлен и доступен.")
        except Exception as e:
            logger.error(f"Failed to create Chrome WebDriver: {e}", exc_info=True)
            self._is_running = False
            error_msg = str(e)
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                raise Exception(f"Не удалось подключиться к Chrome. Проверьте, что Chrome установлен и доступен. Ошибка: {error_msg}")
            elif "cannot find" in error_msg.lower() or "not found" in error_msg.lower():
                raise Exception(f"Chrome или ChromeDriver не найден. Проверьте установку Chrome. Ошибка: {error_msg}")
            raise

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def tab(self) -> SeleniumTab:
        return self._tab

    def navigate(self, url: str) -> None:
        if not self.driver:
            self._initialize_driver()
        self.driver.get(url)
        self.current_url = url

    def get_current_url(self) -> str:
        if not self.driver:
            return ""
        try:
            url = self.driver.current_url
            self.current_url = url
            return url
        except Exception as e:
            logger.warning(f"Error getting current URL: {e}")
            return self.current_url or ""

    def get_page_source(self) -> str:
        if not self.driver:
            return ""
        return self.driver.page_source

    def execute_script(self, script: str, *args) -> Any:
        if not self.driver:
            return None
        return self.driver.execute_script(script, *args)

    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Any]:
        import re
        if not self.driver:
            return None
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                logs = self.driver.get_log('performance')
                for log in logs:
                    message = log.get('message', '')
                    if 'Network.responseReceived' in message and re.search(url_pattern, message):
                        return message
            except Exception:
                pass
            time.sleep(0.1)
        return None

    def get_response_body(self, response_message: str) -> str:
        return ""

    def close(self) -> None:
        if self.driver:
            self.driver.quit()
            self.driver = None
            self._is_running = False

    def stop(self) -> None:
        self.close()

    def start(self) -> None:
        logger.info("start() method called")
        logger.info(f"self.driver is: {self.driver}")
        if not self.driver:
            logger.info("Starting driver initialization...")
            try:
                self._initialize_driver()
                logger.info("Driver started successfully in start() method")
            except Exception as e:
                logger.error(f"Failed to start driver: {e}", exc_info=True)
                raise
        else:
            logger.info("Driver already exists, skipping initialization")
            logger.info("Driver already exists, skipping initialization")

    def set_default_timeout(self, timeout: int) -> None:
        if self._tab:
            self._tab.set_default_timeout(timeout)

    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[Any]:
        if not self.driver:
            return []
        from selenium.webdriver.common.by import By
        by_map = {
            'id': By.ID,
            'name': By.NAME,
            'xpath': By.XPATH,
            'css': By.CSS_SELECTOR,
            'class': By.CLASS_NAME,
            'tag': By.TAG_NAME,
            'link': By.LINK_TEXT,
            'partial_link': By.PARTIAL_LINK_TEXT
        }
        by_type, value = locator
        by = by_map.get(by_type.lower(), By.XPATH)
        try:
            elements = self.driver.find_elements(by, value)
            return elements
        except Exception as e:
            logger.warning(f"Error finding elements by locator {locator}: {e}")
            return []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()