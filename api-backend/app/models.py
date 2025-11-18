from pydantic import BaseModel
from datetime import date
from decimal import Decimal


class Cotacao(BaseModel):
    """Schema de cotação."""
    ativo: str
    data_pregao: date
    abertura: Decimal
    fechamento: Decimal
    maximo: Decimal
    minimo: Decimal
    volume: int
