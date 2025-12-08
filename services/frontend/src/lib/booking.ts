import api from './api';
import {
  Zone,
  Place,
  Slot,
  Booking,
  BookingCreate,
  BookingCreateByTime,
  BookingCancel,
  ZoneCreate,
  ZoneUpdate,
  ZoneCloseRequest,
  ZoneStatistics,
} from '@/types';

export const bookingService = {
  // Получить список зон
  async getZones(): Promise<Zone[]> {
    const response = await api.get('/bookings/zones');
    return response.data;
  },

  // Получить места в зоне
  async getPlacesByZone(zoneId: number): Promise<Place[]> {
    const response = await api.get(`/bookings/zones/${zoneId}/places`);
    return response.data;
  },

  // Получить доступные слоты для места на дату
  async getSlots(placeId: number, date: string): Promise<Slot[]> {
    const response = await api.get(`/bookings/places/${placeId}/slots`, {
      params: { date },
    });
    return response.data;
  },

  // Создать бронирование
  async createBooking(data: BookingCreate): Promise<Booking> {
    const response = await api.post('/bookings/', data);
    return response.data;
  },

  // Создать бронирование по времени
  async createBookingByTime(data: BookingCreateByTime): Promise<Booking> {
    const response = await api.post('/bookings/by-time', data);
    return response.data;
  },

  // Отменить бронирование
  async cancelBooking(data: BookingCancel): Promise<Booking> {
    const response = await api.post('/bookings/cancel', data);
    return response.data;
  },

  // Получить историю бронирований
  async getBookingHistory(filters?: {
    status?: string;
    zone_id?: number;
    date_from?: string;
    date_to?: string;
  }): Promise<Booking[]> {
    const response = await api.get('/bookings/history', {
      params: filters,
    });
    return response.data;
  },

  // Продлить бронирование
  async extendBooking(bookingId: number, extendHours: number = 1, extendMinutes: number = 0): Promise<Booking> {
    const response = await api.post(`/bookings/${bookingId}/extend`, {
      extend_hours: extendHours,
      extend_minutes: extendMinutes,
    });
    return response.data;
  },
};

// Admin функции
export const adminService = {
  // Получить все зоны (включая закрытые) для админа
  async getAdminZones(): Promise<Zone[]> {
    const response = await api.get('/admin/zones');
    return response.data;
  },

  // Создать зону
  async createZone(data: ZoneCreate): Promise<Zone> {
    const response = await api.post('/admin/zones', data); // <-- исправлено (убран /bookings)
    return response.data;
  },

  // Обновить зону
  async updateZone(zoneId: number, data: ZoneUpdate): Promise<Zone> {
    const response = await api.patch(`/admin/zones/${zoneId}`, data); // <-- исправлено
    return response.data;
  },

  // Удалить зону
  async deleteZone(zoneId: number): Promise<void> {
    await api.delete(`/admin/zones/${zoneId}`); // <-- исправлено
  },

  // Закрыть зону на обслуживание
  async closeZone(
    zoneId: number,
    data: ZoneCloseRequest
  ): Promise<Booking[]> {
    const response = await api.post(
      `/admin/zones/${zoneId}/close`, // <-- исправлено
      data
    );
    return response.data;
  },

  // Получить статистику по всем зонам
  async getZonesStatistics(): Promise<ZoneStatistics[]> {
    const response = await api.get('/admin/zones/statistics');
    return response.data;
  },
};