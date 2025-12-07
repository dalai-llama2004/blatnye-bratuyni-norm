from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import declarative_base, relationship

# Базовый класс для всех ORM-моделей
Base = declarative_base()


class Zone(Base):
    __tablename__ = "zones"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    address = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    closure_reason = Column(String(500), nullable=True)  # Причина закрытия зоны
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    # Связь: у зоны есть много мест
    places = relationship(
        "Place",
        back_populates="zone",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Zone id={self.id} name={self.name!r}>"


class Place(Base):
    __tablename__ = "places"

    id = Column(Integer, primary_key=True, index=True)
    zone_id = Column(
        Integer,
        ForeignKey("zones.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    # Связь с зоной
    zone = relationship("Zone", back_populates="places")
    # У места есть слоты
    slots = relationship(
        "Slot",
        back_populates="place",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Place id={self.id} zone_id={self.zone_id} name={self.name!r}>"


class Slot(Base):
    __tablename__ = "slots"
    __table_args__ = (
        # один и тот же интервал времени для одного места нельзя дублировать
        UniqueConstraint(
            "place_id",
            "start_time",
            "end_time",
            name="uq_place_time_interval",
        ),
        Index("ix_slot_place_start", "place_id", "start_time"),
    )

    id = Column(Integer, primary_key=True)
    place_id = Column(
        Integer,
        ForeignKey("places.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    # Этот флаг можно использовать как кеш доступности
    is_available = Column(Boolean, default=True, nullable=False)

    place = relationship("Place", back_populates="slots")
    bookings = relationship(
        "Booking",
        back_populates="slot",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<Slot id={self.id} place_id={self.place_id} "
            f"{self.start_time}–{self.end_time}>"
        )


class Booking(Base):
    __tablename__ = "bookings"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False, index=True)
    slot_id = Column(
        Integer,
        ForeignKey("slots.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # Денормализованные данные для удобства
    zone_name = Column(String(255), nullable=True)
    zone_address = Column(String(255), nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    status = Column(
        String(32),
        default="active",  # active / cancelled / completed
        nullable=False,
        index=True,
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    slot = relationship("Slot", back_populates="bookings")

    def __repr__(self) -> str:
        return f"<Booking id={self.id} user_id={self.user_id} slot_id={self.slot_id}>"
