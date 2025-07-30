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

        # Ensure settings to get a refresh token
        gauth.settings['get_refresh_token'] = True
        gauth.settings['oauth_scope'] = ['https://www.googleapis.com/auth/drive.file']
        gauth.settings['access_type'] = 'offline'
        gauth.settings['prompt'] = 'consent'

        if os.path.exists(self.token_file):
            gauth.LoadCredentialsFile(self.token_file)
        else:
            gauth.LoadClientConfigFile(self.client_config_file)

        if gauth.credentials is None:
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            try:
                gauth.Refresh()
            except Exception as e:
                print("Refresh failed, falling back to re-auth:", e)
                gauth.LocalWebserverAuth()
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

    def list(self) -> list[str]:
        # List all table folders inside the root folder
        query = (
            f"'{self.folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
        folder_list = self.drive.ListFile({'q': query}).GetList()
        return [folder['title'] for folder in folder_list]
    
    def delete(self, table_name: str):
        table_folder = self._get_table_folder(table_name)
        if not table_folder:
            print(f"No table folder named '{table_name}' found.")
            return

        confirm = input(
            f"Are you sure you want to delete the entire table '{table_name}' and all its contents? (y/n): "
        )
        if confirm.lower() != 'y':
            print("Deletion cancelled.")
            return

        try:
            print(f"Deleting folder: {table_folder['title']} (ID: {table_folder['id']})")
            table_folder.Delete()
            print(f"Table '{table_name}' and its metadata have been deleted.")
        except Exception as e:
            print(f"Failed to delete table folder: {e}")