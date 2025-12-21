// Автоматическое обновление страницы при изменении статуса задачи
(function() {
    'use strict';
    
    // Получаем task_id из URL
    const pathParts = window.location.pathname.split('/');
    const taskId = pathParts[pathParts.length - 1];
    
    if (!taskId || taskId === 'tasks') {
        return; // Не страница задачи
    }
    
    // УБРАНО: проверка sessionStorage блокировала обновление статуса
    // Теперь статус будет обновляться всегда
    
    let checkInterval = null;
    let lastStatus = null;
    let hasReloaded = false;
    
    function checkTaskStatus() {
        const base = window.ROOT_PATH || '';
        fetch(base + `/api/task_status/${taskId}`, {
            credentials: 'include',
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(response => {
                if (!response.ok) {
                    throw new Error('Failed to fetch task status');
                }
                return response.json();
            })
            .then(data => {
                const currentStatus = data.status;
                
                // ВСЕГДА обновляем статус на странице, даже если он не изменился
                // Это важно для обновления прогресса
                updateStatusOnPage(data);
                
                // Если статус изменился на COMPLETED или FAILED, обновляем страницу только один раз
                if ((currentStatus === 'COMPLETED' || currentStatus === 'FAILED') && !hasReloaded) {
                    if (lastStatus !== currentStatus && lastStatus !== null) {
                        hasReloaded = true;
                        
                        // Останавливаем проверку перед обновлением
                        if (checkInterval) {
                            clearInterval(checkInterval);
                            checkInterval = null;
                        }
                        
                        // Обновляем страницу через небольшую задержку, чтобы пользователь увидел финальный статус
                        setTimeout(() => {
                            window.location.reload();
                        }, 1000);
                        return; // Выходим, чтобы не обновлять lastStatus
                    }
                } else if (currentStatus === 'COMPLETED' || currentStatus === 'FAILED') {
                    // Статус уже COMPLETED/FAILED, останавливаем проверку
                    if (checkInterval) {
                        clearInterval(checkInterval);
                        checkInterval = null;
                    }
                }
                
                lastStatus = currentStatus;
            })
            .catch(error => {
                console.error('Error checking task status:', error);
                // Показываем ошибку только если это не 404 (задача может быть еще не создана)
                if (error.message && !error.message.includes('404')) {
                    console.error('Failed to fetch task status:', error);
                }
            });
    }
    
    function updateStatusOnPage(data) {
        // Обновляем статус в заголовке
        const statusChip = document.querySelector('.status-chip');
        if (statusChip && data.status) {
            statusChip.textContent = data.status;
            statusChip.className = `status-chip status-${data.status.toLowerCase()}`;
        }
        
        // Обновляем прогресс (используем правильный селектор из HTML)
        const progressElement = document.getElementById('progress-status-text');
        if (progressElement) {
            // ВСЕГДА обновляем прогресс, даже если data.progress пустой
            progressElement.textContent = data.progress || 'Обработка...';
        }
        
        // Обновляем прогресс и расчет времени
        updateProgressStages(data.progress || '');
        
        // Логируем для отладки
        console.log('Status updated:', data.status, 'Progress:', data.progress);
    }
    
    // Хранилище для расчета времени
    let progressHistory = [];
    const MAX_HISTORY = 10; // Храним последние 10 измерений
    
    function updateProgressStages(progressText) {
        if (!progressText) progressText = '';
        
        const progressContainer = document.getElementById('progress-container');
        const progressStatusText = document.getElementById('progress-status-text');
        const progressTimeEstimate = document.getElementById('progress-time-estimate');
        
        const statusChip = document.querySelector('.status-chip');
        const taskStatus = statusChip ? statusChip.textContent.trim().toUpperCase() : '';
        
        if (taskStatus === 'RUNNING' || taskStatus === 'PENDING') {
            if (progressContainer) progressContainer.style.display = 'block';
        } else {
            if (progressContainer) progressContainer.style.display = 'none';
            return;
        }
        
        if (progressStatusText) {
            progressStatusText.textContent = progressText || 'Обработка...';
        }
        
        // Парсим прогресс для расчета времени
        const text = progressText.toLowerCase();
        let currentProgress = 0;
        let totalItems = 0;
        let processedItems = 0;
        
        // Пытаемся извлечь прогресс из текста
        const match = progressText.match(/(\d+)\s*[\/из]\s*(\d+)/);
        if (match) {
            processedItems = parseInt(match[1]);
            totalItems = parseInt(match[2]);
            if (totalItems > 0) {
                currentProgress = Math.min(100, Math.round((processedItems / totalItems) * 100));
            }
        } else {
            // Альтернативные паттерны
            const reviewsMatch = progressText.match(/обработано\s+(\d+)\s*(?:из\s+(\d+)|отзыв)/i);
            if (reviewsMatch) {
                processedItems = parseInt(reviewsMatch[1]);
                totalItems = reviewsMatch[2] ? parseInt(reviewsMatch[2]) : 0;
                if (totalItems > 0) {
                    currentProgress = Math.min(100, Math.round((processedItems / totalItems) * 100));
                }
            } else if (text.includes('агрегация') || text.includes('завершена') || text.includes('completed')) {
                currentProgress = 100;
            } else if (text.includes('сканирование') || text.includes('scanning')) {
                currentProgress = 66;
            } else if (text.includes('поиск') || text.includes('searching') || text.includes('инициализация')) {
                currentProgress = 33;
            } else {
                const cardsMatch = progressText.match(/найдено\s+(\d+)/);
                if (cardsMatch) {
                    currentProgress = 50;
                }
            }
        }
        
        // Сохраняем историю прогресса для расчета скорости
        const now = Date.now();
        progressHistory.push({
            time: now,
            progress: currentProgress,
            processed: processedItems,
            total: totalItems
        });
        
        // Ограничиваем размер истории
        if (progressHistory.length > MAX_HISTORY) {
            progressHistory.shift();
        }
        
        // Рассчитываем ориентировочное время до завершения
        let timeEstimate = '';
        if (progressHistory.length >= 2 && currentProgress > 0 && currentProgress < 100) {
            const first = progressHistory[0];
            const last = progressHistory[progressHistory.length - 1];
            const timeDiff = (last.time - first.time) / 1000; // секунды
            const progressDiff = last.progress - first.progress;
            
            if (progressDiff > 0 && timeDiff > 0) {
                const progressPerSecond = progressDiff / timeDiff;
                const remainingProgress = 100 - currentProgress;
                const secondsRemaining = remainingProgress / progressPerSecond;
                
                if (secondsRemaining > 0 && secondsRemaining < 86400) { // меньше суток
                    const minutes = Math.floor(secondsRemaining / 60);
                    const hours = Math.floor(minutes / 60);
                    const remainingMinutes = minutes % 60;
                    
                    if (hours > 0) {
                        timeEstimate = `Ориентировочное время до завершения: ~${hours} ч. ${remainingMinutes} мин.`;
                    } else if (minutes > 0) {
                        timeEstimate = `Ориентировочное время до завершения: ~${minutes} мин.`;
                    } else {
                        timeEstimate = `Ориентировочное время до завершения: ~${Math.ceil(secondsRemaining)} сек.`;
                    }
                } else {
                    timeEstimate = 'Ориентировочное время до завершения: рассчитывается...';
                }
            } else {
                timeEstimate = 'Ориентировочное время до завершения: рассчитывается...';
            }
        } else {
            timeEstimate = 'Ориентировочное время до завершения: рассчитывается...';
        }
        
        if (progressTimeEstimate) {
            progressTimeEstimate.textContent = timeEstimate;
        }
    }
    
    // Начинаем проверку статуса каждые 2 секунды (увеличена частота обновления)
    if (taskId) {
        console.log('Starting task status checker for task:', taskId);
        // Первая проверка сразу
        checkTaskStatus();
        // Устанавливаем интервал проверки
        checkInterval = setInterval(() => {
            console.log('Checking task status...');
            checkTaskStatus();
        }, 2000);
        console.log('Task status checker started, interval ID:', checkInterval);
    } else {
        console.warn('No task ID found, status checker not started');
    }
    
    // Останавливаем проверку при уходе со страницы
    window.addEventListener('beforeunload', () => {
        if (checkInterval) {
            clearInterval(checkInterval);
        }
    });
})();

