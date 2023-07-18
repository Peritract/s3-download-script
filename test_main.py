"""Tests for the functions in main.py"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.client import BaseClient

from main import get_bucket_names, download_file_from_bucket, download_all_files_from_bucket


def test_get_bucket_names_raises_error_on_invalid_client():
    """Checks that get_bucket_names() raises appropriate errors when passed non-client argument."""

    with pytest.raises(TypeError) as err:
        get_bucket_names(3)

    assert err.value.args[0] == "Invalid argument: s3_client must be a boto3 client"


def test_get_bucket_names_with_valid_client_returns_list_of_strings():
    """Checks that get_bucket_names() returns a list of strings."""

    fake_client = MagicMock()
    fake_client.list_buckets.return_value = {
        "Buckets": [{"Name": "Fake Object"}, {"Name": "Fake Object Jr."}]
    }
    fake_client.__class__ = BaseClient

    result = get_bucket_names(fake_client)

    assert isinstance(result, list)
    assert all(isinstance(x, str) for x in result)


def test_download_file_from_bucket_calls_download_file_correctly():
    """"Checks that download_file_from_bucket() makes appropriate function calls."""

    fake_client = MagicMock()
    fake_download = fake_client.download_file

    download_file_from_bucket(fake_client, "fake_bucket", "fake_key", "fake_folder")

    assert fake_download.call_count == 1
    assert fake_download.called_with("fake_bucket", "fake_key", "fake_folder/fake_key")


@patch("main.download_file_from_bucket")
@patch("os.mkdir")
@patch("os.path.exists")
@patch("main.get_all_items_in_bucket")
def test_download_all_files_from_bucket_creates_folder_if_not_exists(fake_get_all, fake_exists,
                                                                     fake_mkdir, fake_download):
    """Tests that download_all_files_from_bucket() makes calls to create a folder."""

    fake_client = MagicMock()
    fake_get_all.return_value = ["fake_objyle", "fake_objavid", "fake_objerella"]
    fake_exists.return_value = False

    download_all_files_from_bucket(fake_client, "fake_bucket", "fake_folder")

    assert fake_download.call_count == 3
    assert fake_mkdir.call_count == 1
    assert fake_mkdir.called_with("fake_folder")
