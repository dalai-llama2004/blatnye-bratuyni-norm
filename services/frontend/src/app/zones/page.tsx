'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { bookingService } from '@/lib/booking';
import { authService } from '@/lib/auth';
import { BOOKING_CONFIG } from '@/lib/constants';
import { Zone } from '@/types';

export default function ZonesPage() {
  const router = useRouter();
  const [zones, setZones] = useState<Zone[]>([]);
  const [selectedZone, setSelectedZone] = useState<Zone | null>(null);
  const [selectedDate, setSelectedDate] = useState<string>(
    new Date().toISOString().split('T')[0]
  );
  const [startHour, setStartHour] = useState<number>(9);
  const [startMinute, setStartMinute] = useState<number>(0);
  const [endHour, setEndHour] = useState<number>(12);
  const [endMinute, setEndMinute] = useState<number>(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [bookingInProgress, setBookingInProgress] = useState(false);
  const [bookingError, setBookingError] = useState<string>('');
  const [bookingSuccess, setBookingSuccess] = useState<string>('');

  // Генерация массивов для часов и минут
  const hours = Array.from({ length: 24 }, (_, i) => i);
  const minutes = Array.from({ length: 60 / BOOKING_CONFIG.MINUTE_STEP }, (_, i) => i * BOOKING_CONFIG.MINUTE_STEP);

  useEffect(() => {
    loadZones();
  }, []);

  const loadZones = async () => {
    try {
      const data = await bookingService.getZones();
      setZones(data.filter(z => z.is_active));
      setLoading(false);
    } catch (err) {
      setError('Ошибка загрузки зон');
      setLoading(false);
    }
  };

  const handleZoneSelect = (zone: Zone) => {
    setSelectedZone(zone);
    setBookingError('');
    setBookingSuccess('');
  };

  const validateTimeRange = (): string | null => {
    const startTotalMinutes = startHour * 60 + startMinute;
    const endTotalMinutes = endHour * 60 + endMinute;
    
    if (endTotalMinutes <= startTotalMinutes) {
      return 'Время окончания должно быть позже времени начала';
    }
    
    const durationMinutes = endTotalMinutes - startTotalMinutes;
    const durationHours = durationMinutes / 60;
    
    if (durationHours > BOOKING_CONFIG.MAX_BOOKING_HOURS) {
      return `Бронирование не может быть длиннее ${BOOKING_CONFIG.MAX_BOOKING_HOURS} часов`;
    }
    
    return null;
  };

  const handleBooking = async () => {
    if (!authService.isAuthenticated()) {
      router.push('/login');
      return;
    }

    if (!selectedZone) {
      setBookingError('Выберите зону');
      return;
    }

    const validationError = validateTimeRange();
    if (validationError) {
      setBookingError(validationError);
      return;
    }

    setBookingInProgress(true);
    setBookingError('');
    setBookingSuccess('');

    try {
      await bookingService.createBookingByTime({
        zone_id: selectedZone.id,
        date: selectedDate,
        start_hour: startHour,
        start_minute: startMinute,
        end_hour: endHour,
        end_minute: endMinute,
      });
      setBookingSuccess('Бронирование успешно создано!');
      
      // Скрыть сообщение через 3 секунды
      setTimeout(() => setBookingSuccess(''), 3000);
    } catch (err: any) {
      setBookingError(err.response?.data?.detail || 'Ошибка создания бронирования');
    } finally {
      setBookingInProgress(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-lg text-gray-600">Загрузка...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-8">
          Зоны для бронирования
        </h1>

        {error && (
          <div className="mb-4 rounded-md bg-red-50 p-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        {bookingError && (
          <div className="mb-4 rounded-md bg-red-50 p-4">
            <div className="text-sm text-red-700">{bookingError}</div>
          </div>
        )}

        {bookingSuccess && (
          <div className="mb-4 rounded-md bg-green-50 p-4">
            <div className="text-sm text-green-700">{bookingSuccess}</div>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Список зон */}
          <div className="card">
            <h2 className="text-xl font-semibold mb-4">Выберите зону</h2>
            <div className="space-y-2">
              {zones.map((zone) => (
                <button
                  key={zone.id}
                  onClick={() => handleZoneSelect(zone)}
                  className={`w-full text-left p-4 rounded-lg border-2 transition-colors ${
                    selectedZone?.id === zone.id
                      ? 'border-primary-500 bg-primary-50'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="font-medium">{zone.name}</div>
                  {zone.address && (
                    <div className="text-sm text-gray-500 mt-1">{zone.address}</div>
                  )}
                </button>
              ))}
              {zones.length === 0 && (
                <p className="text-gray-500 text-center py-4">Нет доступных зон</p>
              )}
            </div>
          </div>

          {/* Выбор времени */}
          <div className="card">
            <h2 className="text-xl font-semibold mb-4">Доступные слоты</h2>
            {!selectedZone ? (
              <p className="text-gray-500 text-center py-4">Выберите зону</p>
            ) : (
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Дата
                  </label>
                  <input
                    type="date"
                    value={selectedDate}
                    onChange={(e) => setSelectedDate(e.target.value)}
                    min={new Date().toISOString().split('T')[0]}
                    className="input-field"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Приду в
                  </label>
                  <div className="flex space-x-2">
                    <select
                      value={startHour}
                      onChange={(e) => setStartHour(parseInt(e.target.value))}
                      className="input-field flex-1"
                    >
                      {hours.map((h) => (
                        <option key={h} value={h}>
                          {h.toString().padStart(2, '0')} ч
                        </option>
                      ))}
                    </select>
                    <select
                      value={startMinute}
                      onChange={(e) => setStartMinute(parseInt(e.target.value))}
                      className="input-field flex-1"
                    >
                      {minutes.map((m) => (
                        <option key={m} value={m}>
                          {m.toString().padStart(2, '0')} мин
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Уйду в
                  </label>
                  <div className="flex space-x-2">
                    <select
                      value={endHour}
                      onChange={(e) => setEndHour(parseInt(e.target.value))}
                      className="input-field flex-1"
                    >
                      {hours.map((h) => (
                        <option key={h} value={h}>
                          {h.toString().padStart(2, '0')} ч
                        </option>
                      ))}
                    </select>
                    <select
                      value={endMinute}
                      onChange={(e) => setEndMinute(parseInt(e.target.value))}
                      className="input-field flex-1"
                    >
                      {minutes.map((m) => (
                        <option key={m} value={m}>
                          {m.toString().padStart(2, '0')} мин
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="pt-4">
                  <button
                    onClick={handleBooking}
                    disabled={bookingInProgress}
                    className="btn-primary w-full disabled:opacity-50"
                  >
                    {bookingInProgress ? 'Бронирую...' : 'Забронировать'}
                  </button>
                </div>

                <div className="text-sm text-gray-600 mt-2">
                  <p>• Максимальная длительность: {BOOKING_CONFIG.MAX_BOOKING_HOURS} часов</p>
                  <p>• Минуты кратны {BOOKING_CONFIG.MINUTE_STEP}</p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
