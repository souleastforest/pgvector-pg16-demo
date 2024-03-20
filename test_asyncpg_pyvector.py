import asyncio
import logging
import unittest
from unittest.mock import MagicMock

from asyncpg import Connection

from asyncpg_pyvector import init_collection_dict

class TestInitCollectionDict(unittest.TestCase):
    def setUp(self):
        self.pool = MagicMock()

    def test_init_collection_dict(self):
        # Mock the fetch method of the connection to return a list of documents
        documents = [
            (1, "bronya", "document1", "embedding1"),
            (2, "gwen", "document2", "embedding2"),
            (3, "hutao", "document3", "embedding3"),
        ]
        connection = MagicMock(spec=Connection)
        connection.fetch.return_value = documents
        self.pool.acquire.return_value.__aenter__.return_value = connection

        # Mock the encode method of the model to return embeddings
        model = MagicMock()
        model.encode.side_effect = lambda x: f"embedding_{x}"
        
        # Mock the character_config.knowledges dictionary
        character_config = MagicMock()
        character_config.knowledges = {
            "bronya": ["knowledge1", "knowledge2"],
            "gwen": ["knowledge3", "knowledge4"],
            "hutao": ["knowledge5", "knowledge6"],
        }

        # Call the function
        asyncio.run(init_collection_dict(self.pool, model, character_config))

        # Assertions
        self.assertEqual(len(logging.info.mock_calls), 9)  # Check the number of logging calls
        self.assertEqual(self.pool.acquire.call_count, 1)  # Check the number of times pool.acquire is called
        self.assertEqual(connection.fetch.call_count, 1)  # Check the number of times connection.fetch is called
        self.assertEqual(model.encode.call_count, 6)  # Check the number of times model.encode is called
        self.assertEqual(connection.fetch.call_args[0][0], 'SELECT * FROM documents')  # Check the SQL query
        self.assertEqual(
            connection.fetch.call_args[1], {}
        )  # Check that no additional arguments are passed to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call
        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call

        self.assertEqual(
            connection.fetch.call_args_list[0][0][0], 'SELECT * FROM documents'
        )  # Check the SQL query in the first call to connection.fetch
        self.assertEqual(
            connection.fetch.call_args_list[0][1], {}
        )  # Check that no additional arguments are passed to connection.fetch in the first call

        # Check the values in the collection_dict dictionary
        expected_collection_dict = {
            "bronya": {"bronya": ["knowledge1", "knowledge2"]},
            "gwen": {"gwen": ["knowledge3", "knowledge4"]},
            "hutao": {"hutao": ["knowledge5", "knowledge6"]},
        }
        self.assertEqual(collection_dict, expected_collection_dict)

collection_dict = {}

if __name__ == "__main__":
    unittest.main()