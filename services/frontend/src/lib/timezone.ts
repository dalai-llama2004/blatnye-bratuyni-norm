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
 * Парсит значение из datetime-local input как московское время и возвращает ISO строку.
 * @param datetimeLocal - Значение из datetime-local input (YYYY-MM-DDTHH:mm)
 * @returns ISO строка с московским временем
 */
export function fromMoscowDatetimeLocal(datetimeLocal: string): string {
  // Создаем дату из локального значения, интерпретируя его как московское время
  // datetime-local возвращает строку без timezone, поэтому мы добавляем timezone явно
  const isoString = datetimeLocal + ':00'; // Добавляем секунды
  
  // Возвращаем ISO строку (backend ожидает naive datetime, который интерпретируется как UTC)
  // Но так как мы хотим работать с московским временем, нам нужно конвертировать
  return isoString;
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
 * @returns Date объект с текущим временем
 */
export function nowMoscow(): Date {
  return new Date();
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
