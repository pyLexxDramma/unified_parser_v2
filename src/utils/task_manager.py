from __future__ import annotations
import uuid
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class TaskStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    def __init__(self, task_id: str, status: str, progress: str, email: Optional[str] = None,
                 source_info: Optional[Dict[str, Any]] = None):
        self.task_id: str = task_id
        self.status: str = status
        self.progress: str = progress
        self.email: Optional[str] = email
        self.source_info: Optional[Dict[str, Any]] = source_info or {}
        self.detailed_results: List[Dict[str, Any]] = []
        self.statistics: Dict[str, Any] = {}
        self.result_file: Optional[str] = None
        self.error: Optional[str] = None
        self.timestamp: datetime = datetime.now()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        # Для отслеживания времени паузы
        self.pause_start_time: Optional[datetime] = None
        self.total_paused_duration: float = 0.0  # Общее время в паузе в секундах

    def __repr__(self):
        return (f"TaskStatus(task_id='{self.task_id}', status='{self.status}', "
                f"progress='{self.progress}', email='{self.email}', "
                f"source_info={self.source_info})")

active_tasks: Dict[str, TaskStatus] = {}
# Флаги для управления задачами (пауза, остановка)
task_control_flags: Dict[str, Dict[str, bool]] = {}  # {task_id: {"paused": bool, "stopped": bool}}

def create_task(email: Optional[str] = None, source_info: Optional[Dict[str, Any]] = None) -> str:
    task_id = str(uuid.uuid4())
    active_tasks[task_id] = TaskStatus(
        task_id=task_id,
        status=TaskStatus.PENDING,
        progress="Инициализация...",
        email=email,
        source_info=source_info or {}
    )
    task_control_flags[task_id] = {"paused": False, "stopped": False}
    logger.info(f"Created task {task_id} with source_info: {source_info}")
    return task_id

def get_task(task_id: str) -> Optional[TaskStatus]:
    return active_tasks.get(task_id)

def update_task_status(task_id: str, status: str, progress: str = None, error: str = None):
    if task_id in active_tasks:
        task = active_tasks[task_id]
        old_status = task.status
        task.status = status
        if progress is not None:
            task.progress = progress
        if error is not None:
            task.error = error
        
        # Отслеживаем время начала и окончания
        if old_status == TaskStatus.PENDING and status == TaskStatus.RUNNING:
            task.start_time = datetime.now()
        if status in (TaskStatus.COMPLETED, TaskStatus.FAILED) and task.end_time is None:
            task.end_time = datetime.now()
        
        logger.debug(f"Updated task {task_id}: status={status}, progress={progress[:100] if progress else None}")
    else:
        logger.warning(f"Attempted to update non-existent task {task_id}")

def pause_task(task_id: str) -> bool:
    """Приостановить задачу с приостановкой таймера"""
    if task_id in active_tasks and task_id in task_control_flags:
        task = active_tasks[task_id]
        if task.status == TaskStatus.RUNNING:
            task_control_flags[task_id]["paused"] = True
            task.status = TaskStatus.PAUSED
            task.progress = "Приостановлено пользователем"
            # Запоминаем время начала паузы
            task.pause_start_time = datetime.now()
            logger.info(f"Task {task_id} paused by user at {task.pause_start_time}")
            return True
    return False

def resume_task(task_id: str) -> bool:
    """Возобновить задачу с возобновлением таймера"""
    if task_id in active_tasks and task_id in task_control_flags:
        task = active_tasks[task_id]
        if task.status == TaskStatus.PAUSED:
            task_control_flags[task_id]["paused"] = False
            task.status = TaskStatus.RUNNING
            task.progress = "Возобновлено пользователем"
            # Вычисляем время паузы и добавляем к общему времени паузы
            if task.pause_start_time:
                pause_duration = (datetime.now() - task.pause_start_time).total_seconds()
                task.total_paused_duration += pause_duration
                task.pause_start_time = None
                logger.info(f"Task {task_id} resumed by user. Pause duration: {pause_duration:.2f}s, Total paused: {task.total_paused_duration:.2f}s")
            else:
                logger.info(f"Task {task_id} resumed by user")
            return True
    return False

def stop_task(task_id: str) -> bool:
    """Остановить задачу"""
    if task_id in active_tasks and task_id in task_control_flags:
        task_control_flags[task_id]["stopped"] = True
        task_control_flags[task_id]["paused"] = False  # Снимаем паузу при остановке
        logger.info(f"Task {task_id} stop flag set")
        return True
    return False

def is_task_paused(task_id: str) -> bool:
    """Проверить, приостановлена ли задача"""
    return task_control_flags.get(task_id, {}).get("paused", False)

def is_task_stopped(task_id: str) -> bool:
    """Проверить, остановлена ли задача"""
    return task_control_flags.get(task_id, {}).get("stopped", False)

