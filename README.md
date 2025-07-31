# Authentication Setup

1. Go to https://console.cloud.google.com/
2. Create a new project
3. On the left sidebar, APIs & Services -> Library
4. Search and enable `Google Drive API`
5. APIs & Services -> Credentials -> + Create credentials -> OAuth client ID -> Configure consent screen
    - External user type
6. Go back to ... -> OAuth client ID
7. Select Desktop App
8. Download credentials json file
9. Go to https://console.cloud.google.com/apis/credentials/consent
10. Add your email under test users

Config dictionary:

```python
config = {
    "folder_id": <string of characters in drive url after final '/' character>,
    "client_config_file": "client_secrets.json"
}
```

# DataGateway Class Documentation

The `DataGateway` class provides a simple interface to manage tabular data stored as Parquet files on Google Drive. Each table is organized inside its own folder within a specified root folder.

---

## Initialization

`DataGateway(config: dict)`

- **config** (`dict`): Configuration dictionary containing:
  - `folder_id` (`str`): Google Drive folder ID where tables are stored.
  - `client_config_file` (`str`): Path to the OAuth client secrets JSON file.

---

## Methods

### `put(table_name: str, df: pandas.DataFrame, overwrite: bool = False)`

Uploads the given DataFrame as a Parquet file inside the folder for the specified table. Also uploads a metadata Parquet file containing the DataFrame’s info.

- `table_name` (`str`): Name of the table.
- `df` (`pandas.DataFrame`): DataFrame to upload.
- `overwrite` (`bool`, optional): Whether to overwrite existing files. Default is `False`.

Raises `FileExistsError` if the table exists and `overwrite` is `False`.

### `get(table_name: str) -> pandas.DataFrame`

Downloads and returns the DataFrame for the specified table.

- `table_name` (`str`): Name of the table.

Raises `FileNotFoundError` if the table or data file does not exist.

### `meta(table_name: str) -> str`

Retrieves the metadata string (from the DataFrame’s `.info()`) for the specified table.

- `table_name` (`str`): Name of the table.

Raises `FileNotFoundError` if the metadata file does not exist.

### `list() -> list[str]`

Returns a list of all table folder names inside the root folder.

### `delete(table_name: str)`

Deletes the entire folder for the specified table, including all data and metadata files. Prompts the user for confirmation before deleting.

- `table_name` (`str`): Name of the table to delete.

If the folder does not exist, prints a message and aborts the operation.

---

## Notes

- Each table is stored within its own dedicated folder inside the root folder identified by `folder_id`.
- Data and metadata files are stored in Parquet format for efficiency.
- The metadata file contains the output of the DataFrame `.info()` method saved as Parquet.
- Authentication uses OAuth with offline access and token refreshing handled transparently.
