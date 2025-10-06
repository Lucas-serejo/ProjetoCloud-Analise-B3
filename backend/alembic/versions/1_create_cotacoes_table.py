"""create cotacoes table

Revision ID: 1
Revises: 
Create Date: 2025-10-05 22:00:00.000000

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
        sa.Column('ativo', sa.String, nullable=False),
        sa.Column('data_pregao', sa.Date, nullable=False),
        sa.Column('abertura', sa.Numeric(15, 2)),
        sa.Column('fechamento', sa.Numeric(15, 2)),
        sa.Column('maximo', sa.Numeric(15, 2)),
        sa.Column('minimo', sa.Numeric(15, 2)),
        sa.Column('volume', sa.BigInteger),
        sa.Column('timestamp_processamento', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'))
    )
    
    op.create_index('idx_ativo_data', 'cotacoes', ['ativo', 'data_pregao'], unique=True)


def downgrade() -> None:
    op.drop_index('idx_ativo_data', 'cotacoes')
    op.drop_table('cotacoes')