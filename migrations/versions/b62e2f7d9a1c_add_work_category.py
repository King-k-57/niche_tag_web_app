"""add work category

Revision ID: b62e2f7d9a1c
Revises: 431e4fb67890
Create Date: 2026-04-15 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b62e2f7d9a1c"
down_revision = "431e4fb67890"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    return column_name in columns


def upgrade():
    if not _has_column("works", "category"):
        with op.batch_alter_table("works") as batch_op:
            batch_op.add_column(
                sa.Column(
                    "category",
                    sa.String(length=20),
                    nullable=False,
                    server_default="anime",
                )
            )


def downgrade():
    if _has_column("works", "category"):
        with op.batch_alter_table("works") as batch_op:
            batch_op.drop_column("category")
