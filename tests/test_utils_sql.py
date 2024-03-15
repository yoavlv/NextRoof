import pytest
from unittest.mock import MagicMock, patch
from utils.utils_sql import DatabaseManager  # Adjust the import according to your project structure

@pytest.fixture
def mock_db_connection(mocker):
    """Fixture to mock database connection and cursor."""
    conn = mocker.MagicMock()
    cursor = mocker.MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    mocker.patch('DatabaseManager.get_db_connection', return_value=conn)
    return conn, cursor

@pytest.fixture
def database_manager():
    """Fixture for DatabaseManager instance."""
    return DatabaseManager(table_name='test_table')


def test_init(mock_db_connection):
    db_name = 'nextroof_db'
    host_name = 'localhost'
    table_name = 'test_table'
    db_manager = DatabaseManager(table_name=table_name, db_name=db_name, host_name=host_name)
    mock_db_connection[0].assert_called_once_with(db_name, host_name)
    assert db_manager.table_name == table_name
    assert db_manager.conn is not None
