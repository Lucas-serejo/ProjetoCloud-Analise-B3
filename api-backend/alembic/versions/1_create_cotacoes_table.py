"""create cotacoes table

Revision ID: 1
Revises: 
Create Date: 2025-10-05 22:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# Identificadores de revisão usados pelo Alembic.
revision: str = '1'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Cria a tabela 'cotacoes' e índices"""
    op.create_table(
        'cotacoes',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('ativo', sa.String(10), nullable=False),
        sa.Column('data_pregao', sa.Date, nullable=False),
        sa.Column('abertura', sa.Numeric(15, 2), nullable=False),
        sa.Column('fechamento', sa.Numeric(15, 2), nullable=False),
        sa.Column('maximo', sa.Numeric(15, 2), nullable=False),
        sa.Column('minimo', sa.Numeric(15, 2), nullable=False),
        sa.Column('volume', sa.BigInteger, nullable=False, default=0),
        sa.Column('timestamp_processamento', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'))
    )
    
    # Índice único para evitar duplicatas
    op.create_index('idx_ativo_data', 'cotacoes', ['ativo', 'data_pregao'], unique=True)
    
    # Índice para queries rápidas por ativo
    op.create_index('idx_ativo', 'cotacoes', ['ativo'])
    
    # Índice para queries por data
    op.create_index('idx_data_pregao', 'cotacoes', ['data_pregao'])


def downgrade() -> None:
    """Remove a tabela 'cotacoes' e seus índices"""
    op.drop_index('idx_data_pregao', 'cotacoes')
    op.drop_index('idx_ativo', 'cotacoes')
    op.drop_index('idx_ativo_data', 'cotacoes')
    op.drop_table('cotacoes')
