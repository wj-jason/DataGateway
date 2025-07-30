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
