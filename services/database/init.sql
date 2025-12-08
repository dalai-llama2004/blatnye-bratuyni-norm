-- Инициализация схемы пользователей
CREATE SCHEMA IF NOT EXISTS users;

CREATE TABLE IF NOT EXISTS users.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    email VARCHAR(120) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Инициализация схемы бронирований
CREATE SCHEMA IF NOT EXISTS bookings;

CREATE TABLE IF NOT EXISTS bookings.zones (
    id SERIAL PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    address VARCHAR(255) NOT NULL,
    places_count INT,
    closure_reason TEXT DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS bookings.places (
    id SERIAL PRIMARY KEY,
    zone_id INT REFERENCES bookings.zones(id),
    name VARCHAR(120) NOT NULL
);

CREATE TABLE IF NOT EXISTS bookings.slots (
    id SERIAL PRIMARY KEY,
    place_id INT REFERENCES bookings.places(id),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS bookings.bookings (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users.users(id),
    slot_id INT REFERENCES bookings.slots(id),
    status VARCHAR(20) NOT NULL, -- active/cancelled
    created_at TIMESTAMP DEFAULT NOW()
);