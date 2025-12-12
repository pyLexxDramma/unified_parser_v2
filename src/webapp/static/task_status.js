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
        
        // Обновляем прогресс-бар с этапами
        updateProgressStages(data.progress || '');
        
        // Логируем для отладки
        console.log('Status updated:', data.status, 'Progress:', data.progress);
    }
    
    function updateProgressStages(progressText) {
        if (!progressText) progressText = '';
        
        // Обновляем общий прогресс-бар
        const progressContainer = document.getElementById('progress-container');
        const progressStatusText = document.getElementById('progress-status-text');
        const progressBarFill = document.getElementById('progress-bar-fill');
        const progressStagesContainer = document.getElementById('progress-stages-container');
        
        const statusChip = document.querySelector('.status-chip');
        const taskStatus = statusChip ? statusChip.textContent.trim().toUpperCase() : '';
        
        if (taskStatus === 'RUNNING' || taskStatus === 'PENDING') {
            if (progressContainer) progressContainer.style.display = 'block';
            if (progressStagesContainer) progressStagesContainer.style.display = 'block';
        } else {
            if (progressContainer) progressContainer.style.display = 'none';
            if (progressStagesContainer) progressStagesContainer.style.display = 'none';
            return;
        }
        
        if (progressStatusText) {
            progressStatusText.textContent = progressText || 'Обработка...';
        }
        
        // Обновляем общий прогресс-бар
        let progressPercent = 0;
        const match = progressText.match(/(\d+)\s*[\/из]\s*(\d+)/);
        if (match) {
            const current = parseInt(match[1]);
            const total = parseInt(match[2]);
            progressPercent = total > 0 ? Math.round((current / total) * 100) : 0;
        } else if (progressText.includes('Агрегация') || progressText.includes('завершена') || progressText.includes('completed')) {
            progressPercent = 100;
        } else if (progressText.includes('Сканирование') || progressText.includes('Scanning')) {
            progressPercent = 66;
        } else if (progressText.includes('Поиск') || progressText.includes('Searching') || progressText.includes('Инициализация')) {
            progressPercent = 33;
        } else {
            const cardsMatch = progressText.match(/найдено\s+(\d+)/);
            if (cardsMatch) {
                progressPercent = 50;
            }
        }
        
        progressPercent = Math.min(100, Math.max(0, progressPercent));
        if (progressBarFill) {
            progressBarFill.style.width = `${progressPercent}%`;
            progressBarFill.textContent = progressPercent > 0 ? `${progressPercent}%` : '';
        }
        
        // Обновляем отдельные прогресс-бары для каждого этапа
        const text = progressText.toLowerCase();
        const stages = {
            search: { percent: 0, status: 'Ожидание...', active: false },
            filter: { percent: 0, status: 'Ожидание...', active: false },
            parse: { percent: 0, status: 'Ожидание...', active: false },
            reviews: { percent: 0, status: 'Ожидание...', active: false }
        };
        
        // Этап 1: Поиск карточек
        if (text.includes('поиск карточек') || text.includes('инициализация') || text.includes('searching') || (text.includes('найдено') && text.includes('карточк'))) {
            stages.search.active = true;
            stages.search.status = progressText;
            const foundMatch = progressText.match(/(\d+)\s*(?:карточек|cards|найдено)/i);
            if (foundMatch) {
                const found = parseInt(foundMatch[1]);
                stages.search.percent = Math.min(100, (found / 100) * 100);
            } else {
                stages.search.percent = 50;
            }
        } else if (text.includes('завершена') && text.includes('поиск')) {
            stages.search.percent = 100;
            stages.search.status = 'Завершено';
        }
        
        // Этап 2: Ранняя фильтрация по сайту
        if (text.includes('проверка сайтов') || text.includes('ранняя фильтрация') || text.includes('фильтрация по сайту') || text.includes('применяю раннюю')) {
            stages.filter.active = true;
            stages.filter.status = progressText;
            const filterMatch = progressText.match(/(\d+)\/(\d+)/);
            if (filterMatch) {
                const current = parseInt(filterMatch[1]);
                const total = parseInt(filterMatch[2]);
                stages.filter.percent = total > 0 ? Math.round((current / total) * 100) : 0;
            } else if (text.includes('фильтрация завершена')) {
                stages.filter.percent = 100;
                stages.filter.status = 'Завершено';
            } else {
                stages.filter.percent = 10;
            }
        } else if (text.includes('фильтрация завершена') || (text.includes('->') && text.includes('карточек'))) {
            stages.filter.percent = 100;
            stages.filter.status = 'Завершено';
        }
        
        // Этап 3: Парсинг карточек
        if (text.includes('сканирование карточек') || text.includes('processing card') || text.includes('парсинг карточк')) {
            stages.parse.active = true;
            stages.parse.status = progressText;
            const parseMatch = progressText.match(/(\d+)\/(\d+)/);
            if (parseMatch) {
                const current = parseInt(parseMatch[1]);
                const total = parseInt(parseMatch[2]);
                stages.parse.percent = total > 0 ? Math.round((current / total) * 100) : 0;
            } else {
                stages.parse.percent = 10;
            }
        }
        
        // Этап 4: Парсинг отзывов
        if (text.includes('отзыв') || text.includes('review') || text.includes('reviews page') || text.includes('scrolling')) {
            stages.reviews.active = true;
            stages.reviews.status = progressText;
            const reviewsMatch = progressText.match(/(\d+)\s*(?:отзыв|review)/i);
            if (reviewsMatch) {
                const reviews = parseInt(reviewsMatch[1]);
                stages.reviews.percent = Math.min(100, (reviews / 500) * 100);
            } else {
                stages.reviews.percent = 10;
            }
        }
        
        // Обновляем UI для каждого этапа
        ['search', 'filter', 'parse', 'reviews'].forEach(stageName => {
            const stage = stages[stageName];
            const fillEl = document.getElementById(`stage-${stageName}-fill`);
            const percentEl = document.getElementById(`stage-${stageName}-percent`);
            const statusEl = document.getElementById(`stage-${stageName}-status`);
            const stageEl = document.getElementById(`stage-${stageName}`);
            
            if (fillEl) {
                fillEl.style.width = `${stage.percent}%`;
            }
            if (percentEl) {
                percentEl.textContent = stage.percent > 0 ? `${stage.percent}%` : '—';
                percentEl.style.color = stage.active ? '#333' : '#888';
            }
            if (statusEl) {
                statusEl.textContent = stage.status;
                statusEl.style.color = stage.active ? '#2196F3' : '#666';
            }
            if (stageEl) {
                stageEl.style.opacity = stage.active ? '1' : '0.6';
            }
        });
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

