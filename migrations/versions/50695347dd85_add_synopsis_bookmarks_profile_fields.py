"""add synopsis bookmarks profile fields

Revision ID: 50695347dd85
Revises: b62e2f7d9a1c
Create Date: 2026-04-15 21:03:38.746228

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '50695347dd85'
down_revision = 'b62e2f7d9a1c'
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    if not _has_table(table_name):
        return False

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def upgrade():
    if not _has_table('users'):
        op.create_table(
            'users',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('username', sa.String(length=80), nullable=False),
            sa.Column('password_hash', sa.String(length=255), nullable=False),
            sa.Column('is_admin', sa.Boolean(), nullable=False, server_default=sa.text('0')),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('username'),
        )

    if not _has_table('bookmarks'):
        op.create_table(
            'bookmarks',
            sa.Column('user_id', sa.Integer(), nullable=False),
            sa.Column('work_id', sa.Integer(), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
            sa.ForeignKeyConstraint(['user_id'], ['users.id']),
            sa.ForeignKeyConstraint(['work_id'], ['works.id']),
            sa.PrimaryKeyConstraint('user_id', 'work_id'),
        )

    if not _has_column('users', 'created_at'):
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.add_column(
                sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'))
            )

    if not _has_column('works', 'synopsis'):
        with op.batch_alter_table('works', schema=None) as batch_op:
            batch_op.add_column(sa.Column('synopsis', sa.Text(), nullable=True))


def downgrade():
    if _has_column('works', 'synopsis'):
        with op.batch_alter_table('works', schema=None) as batch_op:
            batch_op.drop_column('synopsis')

    if _has_column('users', 'created_at'):
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.drop_column('created_at')

    if _has_table('bookmarks'):
        op.drop_table('bookmarks')

    # usersテーブルは既存環境で既に運用されている可能性があるため、
    # downgradeでは安全のため削除しない。
