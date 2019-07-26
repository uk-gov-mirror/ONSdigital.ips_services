"""003 update tables

Revision ID: 64b719878e71
Revises: 7fe61c4343bb
Create Date: 2019-07-19 09:25:36.858359

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '64b719878e71'
down_revision = '7fe61c4343bb'
branch_labels = None
depends_on = None


def upgrade():
    # Adding extra column to user table example
    op.add_column(
        'USER',
        sa.Column('EXTRA_USER_COLUMN', sa.INTEGER, nullable=True)
    )


def downgrade():
    # Removing extra user column
    op.drop_column('USER', 'EXTRA_USER_COLUMN')
