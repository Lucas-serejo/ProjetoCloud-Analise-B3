"""create cotacoes table

Revision ID: 1
Revises: 
Create Date: 2025-10-01 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'cotacoes',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('data_pregao', sa.Date, nullable=False),
        sa.Column('cod_bdi', sa.String),
        sa.Column('sigla_acao', sa.String, nullable=False),
        sa.Column('tipo_mercado', sa.String),
        sa.Column('nome_empresa', sa.String),
        sa.Column('especificacao_papel', sa.String),
        sa.Column('preco_abertura', sa.Numeric(18, 2)),
        sa.Column('preco_maximo', sa.Numeric(18, 2)),
        sa.Column('preco_minimo', sa.Numeric(18, 2)),
        sa.Column('preco_medio', sa.Numeric(18, 2)),
        sa.Column('preco_fechamento', sa.Numeric(18, 2)),
        sa.Column('quantidade_negociada', sa.BigInteger),
        sa.Column('volume_negociado', sa.Numeric(18, 2)),
    )


def downgrade() -> None:
    op.drop_table('cotacoes')
