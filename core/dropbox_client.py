"""
Dropbox API v2 client — READ-ONLY for existing assets.

Permissions enforced by this client:
  ✅  List / browse folder contents
  ✅  Read and download files
  ✅  Create NEW files or sub-folders (local only — uploads not implemented)
  ❌  Never deletes any file or folder in Dropbox
  ❌  Never overwrites an existing local file

Setup (one-time, ~2 min):
1. https://www.dropbox.com/developers/apps
   → Create App → Scoped access → Full Dropbox → name it "CoffeeAGI"
2. Permissions tab → enable:  files.content.read   files.metadata.read
3. Settings tab → OAuth 2 → Generate access token  (or use refresh token flow)
4. Add to .env:
      DROPBOX_ACCESS_TOKEN=sl.xxxx
      DROPBOX_BRAND_FOLDER=/Maillard/brand          # folder with logos/fonts/guidelines
      DROPBOX_GEORGE_FOLDER=/Maillard/certificates  # George's certificates folder

For refresh-token flow (token expires every 4h):
      DROPBOX_APP_KEY=...
      DROPBOX_APP_SECRET=...
      DROPBOX_REFRESH_TOKEN=...
"""

import json
import os
from pathlib import Path

import httpx
from loguru import logger


class DropboxClient:
    API = "https://api.dropboxapi.com/2"
    CONTENT = "https://content.dropboxapi.com/2"
    TOKEN_URL = "https://api.dropbox.com/oauth2/token"

    def __init__(self):
        self._access_token: str = os.getenv("DROPBOX_ACCESS_TOKEN", "")
        self._refresh_token: str = os.getenv("DROPBOX_REFRESH_TOKEN", "")
        self._app_key: str = os.getenv("DROPBOX_APP_KEY", "")
        self._app_secret: str = os.getenv("DROPBOX_APP_SECRET", "")
        self.brand_folder: str = os.getenv("DROPBOX_BRAND_FOLDER", "/Maillard/brand")
        self.george_folder: str = os.getenv("DROPBOX_GEORGE_FOLDER", "/Maillard/certificates")

    def is_configured(self) -> bool:
        return bool(self._access_token or self._refresh_token)

    def _auth_header(self) -> dict:
        return {"Authorization": f"Bearer {self._access_token}"}

    async def _refresh(self, client: httpx.AsyncClient) -> None:
        if not (self._refresh_token and self._app_key):
            return
        r = await client.post(
            self.TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._app_key,
                "client_secret": self._app_secret,
            },
        )
        if r.status_code == 200:
            self._access_token = r.json()["access_token"]
            logger.info("Dropbox token refreshed.")

    async def _post(self, client: httpx.AsyncClient, endpoint: str, **kwargs) -> httpx.Response:
        """POST with automatic token refresh on 401. Merges auth header with any caller headers."""
        merged = {**self._auth_header(), **kwargs.pop("headers", {})}
        r = await client.post(endpoint, headers=merged, **kwargs)
        if r.status_code == 401:
            await self._refresh(client)
            merged = {**self._auth_header(), **kwargs.pop("headers", {})}
            r = await client.post(endpoint, headers=merged, **kwargs)
        return r

    async def list_folder(self, path: str) -> list[dict]:
        """List files/folders in a Dropbox path. Never modifies anything."""
        async with httpx.AsyncClient(timeout=30) as client:
            r = await self._post(
                client,
                f"{self.API}/files/list_folder",
                headers={**self._auth_header(), "Content-Type": "application/json"},
                json={"path": path, "recursive": False},
            )
        if not r.is_success:
            raise RuntimeError(f"Dropbox list_folder failed: {r.status_code} {r.text[:200]}")

        return [
            {
                "name": e["name"],
                "path": e["path_lower"],
                "size": e.get("size", 0),
                "modified": e.get("client_modified", ""),
                "is_dir": e[".tag"] == "folder",
            }
            for e in r.json().get("entries", [])
        ]

    async def download_file(self, dropbox_path: str) -> bytes:
        """Download a file from Dropbox. Never writes back to Dropbox."""
        async with httpx.AsyncClient(timeout=120) as client:
            r = await self._post(
                client,
                f"{self.CONTENT}/files/download",
                headers={
                    **self._auth_header(),
                    "Dropbox-API-Arg": json.dumps({"path": dropbox_path}),
                },
            )
        if not r.is_success:
            raise RuntimeError(f"Dropbox download failed: {r.status_code} {r.text[:200]}")
        return r.content

    def safe_save(self, dest: Path, content: bytes) -> bool:
        """
        Write content to dest ONLY if the file does not already exist.
        Returns True if saved, False if skipped (file already existed).
        """
        if dest.exists():
            logger.debug(f"Skipped (already exists): {dest}")
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return True
