from __future__ import annotations
import abc
import logging
from typing import Any, List, Optional, Tuple
from selenium.webdriver.remote.webelement import WebElement

logger = logging.getLogger(__name__)


class DOMNode:
    def __init__(self, element_type: str, attributes: dict = None):
        self.element_type = element_type
        self.attributes = attributes or {}

    def add_attribute(self, key: str, value: str):
        self.attributes[key] = value

    def remove_attribute(self, key: str):
        if key in self.attributes:
            del self.attributes[key]

    def has_attribute(self, key: str) -> bool:
        return key in self.attributes


class BaseDriver(abc.ABC):
    @abc.abstractmethod
    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Any]:
        pass

    @abc.abstractmethod
    def get_response_body(self, response: Any) -> Optional[str]:
        pass

    @abc.abstractmethod
    def set_default_timeout(self, timeout: int):
        pass

    @abc.abstractmethod
    def execute_script(self, script: str) -> Any:
        pass

    @abc.abstractmethod
    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[WebElement]:
        pass

    @abc.abstractmethod
    def navigate(self, url: str):
        pass

    @abc.abstractmethod
    def get_current_url(self) -> str:
        pass

    @abc.abstractmethod
    def get_page_source(self) -> str:
        pass

    @abc.abstractmethod
    def stop(self):
        pass

