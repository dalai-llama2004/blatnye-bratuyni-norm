/**
 * Утилиты для работы с московским временем.
 * Все даты и время в приложении используют часовой пояс Europe/Moscow.
 */

// Московский часовой пояс
const MOSCOW_TZ = 'Europe/Moscow';

/**
 * Форматирует дату для datetime-local input в московском времени.
 * @param date - Date объект или строка с датой
 * @returns Строка в формате YYYY-MM-DDTHH:mm для datetime-local input
 */
export function toMoscowDatetimeLocal(date: Date | string): string {
  const d = typeof date === 'string' ? new Date(date) : date;
  
  // Форматируем дату в московском времени
  const moscowTime = d.toLocaleString('sv-SE', {
    timeZone: MOSCOW_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).replace(' ', 'T');
  
  return moscowTime;
}

/**
 * Парсит значение из datetime-local input как московское время и возвращает ISO строку в UTC.
 * @param datetimeLocal - Значение из datetime-local input (YYYY-MM-DDTHH:mm)
 * @returns ISO строка в UTC для отправки на backend (без timezone суффикса)
 */
export function fromMoscowDatetimeLocal(datetimeLocal: string): string {
  // datetime-local возвращает строку без timezone в формате YYYY-MM-DDTHH:mm
  // Мы интерпретируем это значение как московское время
  
  // Добавляем секунды и интерпретируем как московское время
  const dateTimeWithSeconds = datetimeLocal + ':00';
  
  // Парсим строку и создаем Date объект, предполагая что это московское время
  // Для этого мы создаем дату через конструктор и корректируем на московский offset
  const [datePart, timePart] = datetimeLocal.split('T');
  const [year, month, day] = datePart.split('-').map(Number);
  const [hour, minute] = timePart.split(':').map(Number);
  
  // Создаем дату как UTC (избегаем локальной интерпретации)
  const utcDate = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
  
  // Москва это UTC+3, поэтому вычитаем 3 часа чтобы получить UTC время
  const moscowOffsetMs = 3 * 60 * 60 * 1000;
  const actualUtcDate = new Date(utcDate.getTime() - moscowOffsetMs);
  
  // Форматируем как ISO string без timezone (YYYY-MM-DDTHH:mm:ss)
  return actualUtcDate.toISOString().slice(0, 19);
}

/**
 * Форматирует дату для отображения пользователю в московском времени.
 * @param date - Date объект или строка с датой
 * @param options - Опции форматирования (по умолчанию: дата и время)
 * @returns Отформатированная строка
 */
export function formatMoscowTime(
  date: Date | string,
  options?: Intl.DateTimeFormatOptions
): string {
  const d = typeof date === 'string' ? new Date(date) : date;
  
  const defaultOptions: Intl.DateTimeFormatOptions = {
    timeZone: MOSCOW_TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    ...options,
  };
  
  return d.toLocaleString('ru-RU', defaultOptions);
}

/**
 * Возвращает текущее время в московском часовом поясе.
 * @returns Date объект с текущим временем в московском часовом поясе
 */
export function nowMoscow(): Date {
  // Получаем текущее время в московском часовом поясе
  const now = new Date();
  const moscowTimeString = now.toLocaleString('en-US', {
    timeZone: MOSCOW_TZ,
  });
  return new Date(moscowTimeString);
}

/**
 * Получает текущую дату в московском времени в формате YYYY-MM-DD.
 * @returns Строка с датой
 */
export function todayMoscow(): string {
  const now = new Date();
  const moscowDate = now.toLocaleDateString('sv-SE', {
    timeZone: MOSCOW_TZ,
  });
  return moscowDate;
}
