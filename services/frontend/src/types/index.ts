// User types
export interface User {
  id: number;
  name: string;
  email: string;
  confirmed: boolean;
  is_admin: boolean;
  created_at: string;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
}

export interface RegisterData {
  name: string;
  email: string;
  password: string;
}

export interface LoginData {
  email: string;
  password: string;
}

export interface ConfirmData {
  email: string;
  code: string;
}

export interface RecoverData {
  email: string;
}

export interface ResetData {
  email: string;
  code: string;
  new_password: string;
}

// Booking types
export interface Zone {
  id: number;
  name: string;
  address: string | null;
  is_active: boolean;
  closure_reason: string | null;
  closed_until: string | null;
  created_at: string;
  updated_at: string;
}

export interface Place {
  id: number;
  zone_id: number;
  name: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface Slot {
  id: number;
  place_id: number;
  start_time: string;
  end_time: string;
  is_available: boolean;
}

export interface Booking {
  id: number;
  user_id: number;
  slot_id: number;
  zone_name: string | null;
  zone_address: string | null;
  start_time: string | null;
  end_time: string | null;
  status: string;
  cancellation_reason: string | null;
  created_at: string;
  updated_at: string;
}

export interface BookingCreate {
  slot_id: number;
}

export interface BookingCreateByTime {
  zone_id: number;
  date: string;
  start_hour: number;
  start_minute: number;
  end_hour: number;
  end_minute: number;
}

export interface BookingCancel {
  booking_id: number;
}

// Admin types
export interface ZoneCreate {
  name: string;
  address?: string;
  is_active?: boolean;
  places_count: number;
}

export interface ZoneUpdate {
  name?: string;
  address?: string;
  is_active?: boolean;
}

export interface ZoneCloseRequest {
  reason: string;
  from_time: string;
  to_time: string;
}

export interface ZoneStatistics {
  zone_id: number;
  zone_name: string;
  is_active: boolean;
  closure_reason: string | null;
  closed_until: string | null;
  active_bookings: number;
  cancelled_bookings: number;
  current_occupancy: number;
}
