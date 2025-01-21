"""Update thesis table

Revision ID: 1d1cba1b5580
Revises: d12a2ecdd039
Create Date: 2025-01-21 14:27:55.134128

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import sqlite

# revision identifiers, used by Alembic.
revision: str = '1d1cba1b5580'
down_revision: Union[str, None] = 'd12a2ecdd039'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP TABLE IF EXISTS thesis_new')
    op.create_table(
        'thesis_new',
        sa.Column('date', sa.Date, primary_key=True),
        sa.Column('author', sa.String, primary_key=True),
        sa.Column('ticker', sa.String, primary_key=True),
        sa.Column('company_name', sa.String, nullable=True),
        sa.Column('link', sa.String, nullable=True),
        sa.Column('market_cap', sa.Float, nullable=True),
        sa.Column('price', sa.Float, nullable=True),
        sa.Column('created_at', sa.DateTime, default=sa.func.now()),
        sa.Column('text', sa.Text, nullable=True),
        sa.Column('profile', sa.JSON, nullable=True),
        sa.Column('daily_price', sa.JSON, nullable=True),
        sa.Column('dividends', sa.JSON, nullable=True)
    )
    op.execute('INSERT INTO thesis_new (date, author, ticker, company_name, link, market_cap, price, created_at, text, profile, daily_price, dividends) SELECT date, author, ticker, company_name, link, market_cap, price, created_at, text, profile, monthly_performance, dividends FROM thesis')
    op.drop_table('thesis')
    op.rename_table('thesis_new', 'thesis')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP TABLE IF EXISTS thesis_old')
    op.create_table(
        'thesis_old',
        sa.Column('date', sa.Date, primary_key=True),
        sa.Column('author', sa.String, primary_key=True),
        sa.Column('ticker', sa.String, primary_key=True),
        sa.Column('company_name', sa.String, nullable=True),
        sa.Column('link', sa.String, nullable=True),
        sa.Column('market_cap', sa.Float, nullable=True),
        sa.Column('price', sa.Float, nullable=True),
        sa.Column('created_at', sa.DateTime, default=sa.func.now()),
        sa.Column('text', sa.Text, nullable=True),
        sa.Column('profile', sa.TEXT, nullable=True),
        sa.Column('monthly_performance', sa.JSON, nullable=True),
        sa.Column('dividends', sa.JSON, nullable=True)
    )
    op.execute('INSERT INTO thesis_old (date, author, ticker, company_name, link, market_cap, price, created_at, text, profile, monthly_performance, dividends) SELECT date, author, ticker, company_name, link, market_cap, price, created_at, text, profile, daily_price, dividends FROM thesis')
    op.drop_table('thesis')
    op.rename_table('thesis_old', 'thesis')
    # ### end Alembic commands ###
