import os
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from io import BytesIO

class DataGateway:
    def __init__(self, config: dict):
        """
        Initialize DataGateway using a config dictionary.

        Config should include:
        - folder_id (str): Google Drive folder ID
        - client_config_file (str): Path to client_secrets.json
        - token_file (str): Path to token.json (optional)
        """
        self.folder_id = config["folder_id"]
        self.client_config_file = config["client_config_file"]
        self.token_file = config.get("token_file", "token.json")
        self.drive = self._authenticate()
    
    def _authenticate(self, credentials_path):
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("token.json")

        if gauth.credentials is None:
            gauth.LoadClientConfigFile(credentials_path)
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()

        gauth.SaveCredentialsFile("token.json")
        return GoogleDrive(gauth)

    def _find_file(self, filename):
        query = f"'{self.folder_id}' in parents and title = '{filename}' and trashed = false"
        file_list = self.drive.ListFile({'q': query}).GetList()
        return file_list[0] if file_list else None

    def get(self, table_name: str) -> pd.DataFrame:
        """
        Retrieve a table by name.
        """
        file = self._find_file(f"{table_name}.parquet")
        if not file:
            raise FileNotFoundError(f"Table '{table_name}' not found.")
        content = file.GetContentBinary()
        return pd.read_parquet(BytesIO(content))

    def meta(self, table_name: str) -> str:
        """
        Retrieve the df.info() metadata for a table as a string.
        """
        file = self._find_file(f"{table_name}_meta.parquet")
        if not file:
            raise FileNotFoundError(f"Metadata for table '{table_name}' not found.")
        content = file.GetContentBinary()
        meta_df = pd.read_parquet(BytesIO(content))
        return meta_df.iloc[0]["df_info"]

    def put(self, table_name: str, df: pd.DataFrame):
        """
        Upload a new table and auto-generated metadata.
        """
        buf = BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)

        file = self.drive.CreateFile({'title': f"{table_name}.parquet", 'parents': [{'id': self.folder_id}]})
        file.SetContentString(buf.read().decode("latin1"))
        file.Upload()

        # metadata from df.info()
        info_buf = StringIO()
        df.info(buf=info_buf)
        info_str = info_buf.getvalue()

        meta_df = pd.DataFrame({'df_info': [info_str]})
        buf_meta = BytesIO()
        meta_df.to_parquet(buf_meta, index=False)
        buf_meta.seek(0)

        meta_file = self.drive.CreateFile({'title': f"{table_name}_meta.parquet", 'parents': [{'id': self.folder_id}]})
        meta_file.SetContentString(buf_meta.read().decode("latin1"))
        meta_file.Upload()


    def list(self) -> list:
        """
        List all base table names (excluding metadata files).
        """
        query = f"'{self.folder_id}' in parents and trashed = false"
        files = self.drive.ListFile({'q': query}).GetList()
        table_names = [f['title'].replace('.parquet', '') for f in files if not f['title'].endswith('_meta.parquet')]
        return list(set(table_names))
