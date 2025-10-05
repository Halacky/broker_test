from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Any, Dict, Union


__all__ = ["Product", "Truck", "Area"]


@dataclass
class Product:
    """Модель продукта.

    Поля:
      - first_interaction_at: время первого взаимодействия
      - last_interaction_at: время последнего взаимодействия (может быть пустым)
      - internal_id: внутренний идентификатор (обязательный)
      - external_id: внешний идентификатор (может быть пустым)
      - target_area: целевая зона (может быть пустым)
      - fact_area: фактическая зона (может быть пустым)
      - type: тип продукта (строка)

    Методы:
      - create: Создать сущность
      - update: Изменить сущность
      - get_state: Получить состояние всех её полей
      - delete: Удалить сущность (логическая очистка полей, где это уместно)
    """

    first_interaction_at: datetime
    internal_id: str
    type: str
    last_interaction_at: Optional[datetime] = None
    external_id: Optional[str] = None
    target_area: Optional[str] = None
    fact_area: Optional[str] = None

    @classmethod
    def create(
        cls,
        *,
        internal_id: str,
        type: str,
        first_interaction_at: Optional[datetime] = None,
        last_interaction_at: Optional[datetime] = None,
        external_id: Optional[str] = None,
        target_area: Optional[str] = None,
        fact_area: Optional[str] = None,
    ) -> "Product":
        """Создаёт новый экземпляр продукта."""
        if not internal_id:
            raise ValueError("internal_id обязателен для Product")
        if not type:
            raise ValueError("type обязателен для Product")
        return cls(
            first_interaction_at=first_interaction_at or datetime.now(),
            internal_id=internal_id,
            type=type,
            last_interaction_at=last_interaction_at,
            external_id=external_id,
            target_area=target_area,
            fact_area=fact_area,
        )

    def update(self, **fields: Any) -> None:
        """Обновляет поля сущности. Неизвестные поля игнорируются.

        Пример: product.update(external_id="X", fact_area="zone_1")
        """
        allowed_fields = {
            "first_interaction_at",
            "last_interaction_at",
            "internal_id",
            "external_id",
            "target_area",
            "fact_area",
            "type",
        }
        for key, value in fields.items():
            if key in allowed_fields:
                setattr(self, key, value)

    def get_state(self) -> Dict[str, Any]:
        """Возвращает состояние всех полей сущности в виде словаря."""
        return asdict(self)

    def delete(self) -> bool:
        """Удаляет сущность логически.

        Для простоты выполняется очистка опциональных полей и фиксация времени
        последнего взаимодействия. Возвращает True при успешной операции.
        """
        self.external_id = None
        self.target_area = None
        self.fact_area = None
        self.last_interaction_at = datetime.now()
        return True


@dataclass
class Truck:
    """Модель грузовика (truck).

    Поля:
      - last_modified_at: время последнего изменения
      - id: идентификатор грузовика
      - actual_state: текущее состояние грузовика
      - products: список сущностей Product (может быть пустым)

    Методы:
      - create: Создать сущность
      - update: Изменить сущность
      - get_state: Получить состояние всех её полей
    """

    id: str
    actual_state: int
    products: List[Product] = field(default_factory=list)
    last_modified_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def create(
        cls,
        *,
        id: str,
        actual_state: int,
        products: Optional[List[Product]] = None,
    ) -> "Truck":
        """Создаёт новый экземпляр грузовика."""
        if not id:
            raise ValueError("id обязателен для Truck")
        return cls(id=id, actual_state=actual_state, products=list(products or []), last_modified_at=datetime.now())

    def update(self, *, id: Optional[str] = None, actual_state: Optional[int] = None, products: Optional[List[Product]] = None) -> None:
        """Обновляет поля сущности Truck и фиксирует время изменения."""
        if id is not None:
            if not id:
                raise ValueError("id не может быть пустым")
            self.id = id
        if actual_state is not None:
            self.actual_state = actual_state
        if products is not None:
            self.products = list(products)
        self.last_modified_at = datetime.now()

    def get_state(self) -> Dict[str, Any]:
        """Возвращает состояние всех полей, продукты представлены как словари."""
        return {
            "id": self.id,
            "last_modified_at": self.last_modified_at,
            "actual_state": self.actual_state,
            "products": [p.get_state() for p in self.products],
        }


@dataclass
class Area:
    """Модель зоны (area).

    Поля:
      - products: список сущностей Product

    Методы:
      - add_product: добавить продукт
      - delete_product: удалить продукт
    """

    products: List[Product] = field(default_factory=list)

    def add_product(self, product: Product) -> None:
        """Добавляет продукт, если такого internal_id ещё нет в списке."""
        if not isinstance(product, Product):
            raise TypeError("Ожидается экземпляр Product")
        if any(existing.internal_id == product.internal_id for existing in self.products):
            return  # не дублируем
        self.products.append(product)

    def delete_product(self, product: Union[Product, str]) -> bool:
        """Удаляет продукт по экземпляру или по internal_id. Возвращает True, если удалено."""
        internal_id: Optional[str]
        if isinstance(product, Product):
            internal_id = product.internal_id
        elif isinstance(product, str):
            internal_id = product
        else:
            raise TypeError("Ожидается Product или str (internal_id)")

        for idx, existing in enumerate(self.products):
            if existing.internal_id == internal_id:
                del self.products[idx]
                return True
        return False


