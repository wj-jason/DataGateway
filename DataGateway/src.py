import os
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from io import BytesIO, StringIO
import tempfile
import datetime

class DataGateway:
    def __init__(self, config: dict):
        """
        Initialize DataGateway using a config dictionary.

        Config should include:
        - folder_id (str): Google Drive folder ID
        - client_config_file (str): Path to client_secrets.json
        - token_file (str): Path to token.json
        """
        self.folder_id = config["folder_id"]
        self.client_config_file = config["client_config_file"]
        self.token_file = "token.json"
        self.drive = self._authenticate()

    def _authenticate(self):
        gauth = GoogleAuth()
        if os.path.exists(self.token_file):
            gauth.LoadCredentialsFile(self.token_file)
        else:
            gauth.LoadClientConfigFile(self.client_config_file)

        if gauth.credentials is None:
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()

        gauth.SaveCredentialsFile(self.token_file)
        return GoogleDrive(gauth)

    def _get_or_create_table_folder(self, table_name: str):
        """Get or create a subfolder inside root folder named after the table."""
        query = (
            f"'{self.folder_id}' in parents and title = '{table_name}' "
            f"and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        folder_list = self.drive.ListFile({'q': query}).GetList()
        if folder_list:
            return folder_list[0]
        else:
            folder_metadata = {
                'title': table_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [{'id': self.folder_id}]
            }
            folder = self.drive.CreateFile(folder_metadata)
            folder.Upload()
            return folder

    def _get_table_folder(self, table_name: str):
        """Get the folder for a table if exists, else None."""
        query = (
            f"'{self.folder_id}' in parents and title = '{table_name}' "
            f"and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        folder_list = self.drive.ListFile({'q': query}).GetList()
        return folder_list[0] if folder_list else None

    def _find_file(self, filename: str, parent_folder_id: str):
        """Find a file by name inside a given folder."""
        query = f"'{parent_folder_id}' in parents and title = '{filename}' and trashed = false"
        files = self.drive.ListFile({'q': query}).GetList()
        return files[0] if files else None

    def _log_update(self, table_name: str, action: str, rows: int, folder_id: str):
        """Append a log line to table.log inside the table folder."""
        log_filename = f"{table_name}.log"
        now = datetime.datetime.utcnow().isoformat() + "Z"
        log_line = f"{now} | {action} | {table_name} | rows: {rows}\n"

        existing_log_file = self._find_file(log_filename, folder_id)
        if existing_log_file:
            with tempfile.NamedTemporaryFile("r+", suffix=".log", delete=False) as tmp_log:
                existing_log_file.GetContentFile(tmp_log.name)
                tmp_log.seek(0, 2)  # Move to EOF
                tmp_log.write(log_line)
                tmp_log.flush()

                existing_log_file.SetContentFile(tmp_log.name)
                existing_log_file.Upload()
        else:
            with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as tmp_log:
                tmp_log.write(log_line)
                tmp_log.flush()

                new_log_file = self.drive.CreateFile({'title': log_filename, 'parents': [{'id': folder_id}]})
                new_log_file.SetContentFile(tmp_log.name)
                new_log_file.Upload()

    def put(self, table_name: str, df: pd.DataFrame, overwrite: bool = False):
        # Get or create table folder
        table_folder = self._get_or_create_table_folder(table_name)
        folder_id = table_folder['id']

        # Upload main parquet file
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            df.to_parquet(tmp.name, index=False)

            existing_file = self._find_file(f"{table_name}.parquet", folder_id)
            if existing_file:
                if overwrite:
                    existing_file.SetContentFile(tmp.name)
                    existing_file.Upload()
                else:
                    raise FileExistsError(f"Table {table_name} already exists.")
            else:
                new_file = self.drive.CreateFile({'title': f"{table_name}.parquet", 'parents': [{'id': folder_id}]})
                new_file.SetContentFile(tmp.name)
                new_file.Upload()

        # Upload metadata parquet file
        info_buf = StringIO()
        df.info(buf=info_buf)
        info_str = info_buf.getvalue()
        meta_df = pd.DataFrame({'df_info': [info_str]})

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_meta:
            meta_df.to_parquet(tmp_meta.name, index=False)

            existing_meta_file = self._find_file(f"{table_name}_meta.parquet", folder_id)
            if existing_meta_file:
                existing_meta_file.SetContentFile(tmp_meta.name)
                existing_meta_file.Upload()
            else:
                meta_file = self.drive.CreateFile({'title': f"{table_name}_meta.parquet", 'parents': [{'id': folder_id}]})
                meta_file.SetContentFile(tmp_meta.name)
                meta_file.Upload()

        self._log_update(table_name, "PUT", len(df), folder_id)

    def get(self, table_name: str) -> pd.DataFrame:
        table_folder = self._get_table_folder(table_name)
        if not table_folder:
            raise FileNotFoundError(f"Table folder '{table_name}' does not exist.")

        folder_id = table_folder['id']
        file = self._find_file(f"{table_name}.parquet", folder_id)
        if not file:
            raise FileNotFoundError(f"Table file '{table_name}.parquet' not found in folder.")

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            file.GetContentFile(tmp.name)
            df = pd.read_parquet(tmp.name)
        return df

    def meta(self, table_name: str) -> str:
        table_folder = self._get_table_folder(table_name)
        if not table_folder:
            raise FileNotFoundError(f"Table folder '{table_name}' does not exist.")

        folder_id = table_folder['id']
        file = self._find_file(f"{table_name}_meta.parquet", folder_id)
        if not file:
            raise FileNotFoundError(f"Metadata file '{table_name}_meta.parquet' not found in folder.")

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            file.GetContentFile(tmp.name)
            meta_df = pd.read_parquet(tmp.name)

        return meta_df.iloc[0]["df_info"]

    def append(self, table_name: str, new_rows: pd.DataFrame):
        table_folder = self._get_table_folder(table_name)
        if not table_folder:
            raise FileNotFoundError(f"Table folder '{table_name}' does not exist.")
        folder_id = table_folder['id']

        # Load existing table
        existing_df = self.get(table_name)

        # Check columns match exactly
        if list(existing_df.columns) != list(new_rows.columns):
            raise ValueError("Column names do not match existing table.")

        # Check dtypes match
        for col in existing_df.columns:
            if existing_df[col].dtype != new_rows[col].dtype:
                raise ValueError(
                    f"Column '{col}' dtype mismatch: existing {existing_df[col].dtype}, new {new_rows[col].dtype}"
                )

        # Append new rows
        updated_df = pd.concat([existing_df, new_rows], ignore_index=True)

        # Overwrite parquet file
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            updated_df.to_parquet(tmp.name, index=False)

            existing_file = self._find_file(f"{table_name}.parquet", folder_id)
            existing_file.SetContentFile(tmp.name)
            existing_file.Upload()

        # Overwrite metadata
        info_buf = StringIO()
        updated_df.info(buf=info_buf)
        info_str = info_buf.getvalue()
        meta_df = pd.DataFrame({'df_info': [info_str]})

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_meta:
            meta_df.to_parquet(tmp_meta.name, index=False)

            existing_meta_file = self._find_file(f"{table_name}_meta.parquet", folder_id)
            existing_meta_file.SetContentFile(tmp_meta.name)
            existing_meta_file.Upload()

        self._log_update(table_name, "APPEND", len(new_rows), folder_id)

    def list(self) -> list[str]:
        # List all table folders inside the root folder
        query = (
            f"'{self.folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        folder_list = self.drive.ListFile({'q': query}).GetList()
        return [folder['title'] for folder in folder_list]
    
    def delete(self, table_name: str, selection):
        """
        Delete rows matching the selection from the table.

        Parameters:
        - table_name: str, name of the table
        - selection: function or boolean mask to select rows to delete.
                    If function, called as selection(df) -> boolean mask.
                    If boolean Series/DataFrame mask, directly used.

        Prompts user for confirmation before deletion.
        """

        table_folder = self._get_table_folder(table_name)
        if not table_folder:
            raise FileNotFoundError(f"Table folder '{table_name}' does not exist.")
        folder_id = table_folder['id']

        df = self.get(table_name)

        # Determine rows to delete
        if callable(selection):
            mask = selection(df)
            if not isinstance(mask, pd.Series) or mask.dtype != bool:
                raise ValueError("Selection function must return a boolean pandas Series.")
        elif isinstance(selection, pd.Series) and selection.dtype == bool and len(selection) == len(df):
            mask = selection
        else:
            raise ValueError("Selection must be a function or a boolean pandas Series with same length as table.")

        rows_to_delete = df[mask]

        if rows_to_delete.empty:
            print("No rows match the deletion criteria. Nothing to delete.")
            return

        # Show summary of rows to delete
        print(f"Warning: You are about to delete {len(rows_to_delete)} rows from table '{table_name}':")
        print(rows_to_delete)
        confirm = input("Type 'yes' to confirm deletion: ").strip().lower()
        if confirm != 'yes':
            print("Deletion cancelled.")
            return

        # Keep rows NOT matching mask (i.e. drop selected rows)
        new_df = df[~mask].reset_index(drop=True)

        # Overwrite parquet file
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            new_df.to_parquet(tmp.name, index=False)

            existing_file = self._find_file(f"{table_name}.parquet", folder_id)
            existing_file.SetContentFile(tmp.name)
            existing_file.Upload()

        # Overwrite metadata
        info_buf = StringIO()
        new_df.info(buf=info_buf)
        info_str = info_buf.getvalue()
        meta_df = pd.DataFrame({'df_info': [info_str]})

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_meta:
            meta_df.to_parquet(tmp_meta.name, index=False)

            existing_meta_file = self._find_file(f"{table_name}_meta.parquet", folder_id)
            existing_meta_file.SetContentFile(tmp_meta.name)
            existing_meta_file.Upload()

        self._log_update(table_name, "DELETE", len(rows_to_delete), folder_id)
        print(f"Deleted {len(rows_to_delete)} rows from table '{table_name}'.")