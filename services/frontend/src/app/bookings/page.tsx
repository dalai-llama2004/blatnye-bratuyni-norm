'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { bookingService } from '@/lib/booking';
import { authService } from '@/lib/auth';
import { Booking } from '@/types';
import { formatMoscowTime } from '@/lib/timezone';

export default function BookingsPage() {
  const router = useRouter();
  const [bookings, setBookings] = useState<Booking[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [filters, setFilters] = useState({
    status: '',
    date_from: '',
    date_to: '',
  });
  const [cancellingId, setCancellingId] = useState<number | null>(null);

  useEffect(() => {
    if (!authService.isAuthenticated()) {
      router.push('/login');
      return;
    }
    loadBookings();
  }, []);

  const loadBookings = async () => {
    try {
      const data = await bookingService.getBookingHistory();
      setBookings(data);
      setLoading(false);
    } catch (err: any) {
      if (err.response?.status === 401) {
        router.push('/login');
      } else {
        setError('Ошибка загрузки истории бронирований');
      }
      setLoading(false);
    }
  };

  const handleFilterChange = (key: string, value: string) => {
    setFilters({ ...filters, [key]: value });
  };

  const applyFilters = async () => {
    setLoading(true);
    try {
      const filterParams: any = {};
      if (filters.status) filterParams.status = filters.status;
      if (filters.date_from) filterParams.date_from = filters.date_from;
      if (filters.date_to) filterParams.date_to = filters.date_to;

      const data = await bookingService.getBookingHistory(filterParams);
      setBookings(data);
    } catch (err) {
      setError('Ошибка применения фильтров');
    } finally {
      setLoading(false);
    }
  };

  const resetFilters = () => {
    setFilters({
      status: '',
      date_from: '',
      date_to: '',
    });
    loadBookings();
  };

  const handleCancelBooking = async (bookingId: number) => {
    if (!confirm('Вы уверены, что хотите отменить бронирование?')) {
      return;
    }

    setCancellingId(bookingId);

    try {
      await bookingService.cancelBooking({ booking_id: bookingId });
      alert('Бронирование отменено');
      loadBookings();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Ошибка отмены бронирования');
    } finally {
      setCancellingId(null);
    }
  };

  const handleExtendBooking = async (bookingId: number) => {
    try {
      await bookingService.extendBooking(bookingId);
      alert('Бронирование продлено');
      loadBookings();
    } catch (err: any) {
      alert(err.response?.data?.detail || 'Ошибка продления бронирования');
    }
  };

  const getStatusBadgeClass = (status: string) => {
    switch (status) {
      case 'active':
        return 'bg-green-100 text-green-800';
      case 'cancelled':
        return 'bg-red-100 text-red-800';
      case 'completed':
        return 'bg-gray-100 text-gray-800';
      default:
        return 'bg-blue-100 text-blue-800';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'active':
        return 'Активно';
      case 'cancelled':
        return 'Отменено';
      case 'completed':
        return 'Завершено';
      default:
        return status;
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
          История бронирований
        </h1>

        {error && (
          <div className="mb-4 rounded-md bg-red-50 p-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        {/* Фильтры */}
        <div className="card mb-6">
          <h2 className="text-lg font-semibold mb-4">Фильтры</h2>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Статус
              </label>
              <select
                value={filters.status}
                onChange={(e) => handleFilterChange('status', e.target.value)}
                className="input-field"
              >
                <option value="">Все</option>
                <option value="active">Активно</option>
                <option value="cancelled">Отменено</option>
                <option value="completed">Завершено</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Дата с
              </label>
              <input
                type="date"
                value={filters.date_from}
                onChange={(e) => handleFilterChange('date_from', e.target.value)}
                className="input-field"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Дата по
              </label>
              <input
                type="date"
                value={filters.date_to}
                onChange={(e) => handleFilterChange('date_to', e.target.value)}
                className="input-field"
              />
            </div>

            <div className="flex items-end space-x-2">
              <button onClick={applyFilters} className="btn-primary">
                Применить
              </button>
              <button onClick={resetFilters} className="btn-secondary">
                Сбросить
              </button>
            </div>
          </div>
        </div>

        {/* Список бронирований */}
        <div className="space-y-4">
          {bookings.map((booking) => (
            <div key={booking.id} className="card">
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <div className="flex items-center space-x-3 mb-2">
                    <h3 className="text-lg font-semibold">
                      Бронирование #{booking.id}
                    </h3>
                    <span
                      className={`px-3 py-1 rounded-full text-sm font-medium ${getStatusBadgeClass(
                        booking.status
                      )}`}
                    >
                      {getStatusText(booking.status)}
                    </span>
                  </div>
                  <div className="text-sm text-gray-600 space-y-1">
                    {booking.zone_name && (
                      <p>
                        <span className="font-medium">Зона:</span> {booking.zone_name}
                      </p>
                    )}
                    {booking.zone_address && (
                      <p>
                        <span className="font-medium">Адрес:</span> {booking.zone_address}
                      </p>
                    )}
                    {booking.start_time && booking.end_time && (
                      <p>
                        <span className="font-medium">Время бронирования:</span> с{' '}
                        {formatMoscowTime(booking.start_time)} по{' '}
                        {formatMoscowTime(booking.end_time)}
                      </p>
                    )}
                    <p>Слот ID: {booking.slot_id}</p>
                    <p>
                      Создано:{' '}
                      {formatMoscowTime(booking.created_at)}
                    </p>
                    <p>
                      Обновлено:{' '}
                      {formatMoscowTime(booking.updated_at)}
                    </p>
                    {booking.status === 'cancelled' && booking.cancellation_reason && (
                      <div className="mt-2 p-2 bg-red-50 border border-red-200 rounded">
                        <p className="text-sm text-red-800">
                          <span className="font-medium">Причина отмены:</span> {booking.cancellation_reason}
                        </p>
                      </div>
                    )}
                  </div>
                </div>

                {booking.status === 'active' && (
                  <div className="flex space-x-2">
                    <button
                      onClick={() => handleExtendBooking(booking.id)}
                      className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700"
                    >
                      Продлить
                    </button>
                    <button
                      onClick={() => handleCancelBooking(booking.id)}
                      disabled={cancellingId === booking.id}
                      className="px-4 py-2 text-sm bg-red-600 text-white rounded-md hover:bg-red-700 disabled:opacity-50"
                    >
                      {cancellingId === booking.id ? 'Отмена...' : 'Отменить'}
                    </button>
                  </div>
                )}
              </div>
            </div>
          ))}

          {bookings.length === 0 && (
            <div className="card text-center py-8">
              <p className="text-gray-500">Нет бронирований</p>
              <button
                onClick={() => router.push('/zones')}
                className="mt-4 btn-primary"
              >
                Создать бронирование
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
