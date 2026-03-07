#!/usr/bin/env python3
"""Globus SDK wrapper for listing and downloading files from a Globus endpoint."""

import json
import os
import random
from pathlib import Path

import globus_sdk

TOKEN_CACHE = Path.home() / ".globus_tokens.json"
TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"


class GlobusDownloader:
    def __init__(
        self,
        client_id: str,
        source_endpoint: str,
        local_endpoint: str,
        local_dir: str,
    ):
        self.source_endpoint = source_endpoint
        self.local_endpoint = local_endpoint
        self.local_dir = Path(local_dir).resolve()
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self._transfer_client = self._get_transfer_client(client_id)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _get_transfer_client(self, client_id: str) -> globus_sdk.TransferClient:
        auth_client = globus_sdk.NativeAppAuthClient(client_id)
        tokens = self._load_tokens()

        if tokens:
            authorizer = globus_sdk.RefreshTokenAuthorizer(
                tokens["refresh_token"],
                auth_client,
                access_token=tokens["access_token"],
                expires_at=tokens.get("expires_at_seconds"),
                on_refresh=self._save_tokens,
            )
        else:
            authorizer = self._interactive_login(auth_client)

        return globus_sdk.TransferClient(authorizer=authorizer)

    def _interactive_login(
        self, auth_client: globus_sdk.NativeAppAuthClient
    ) -> globus_sdk.RefreshTokenAuthorizer:
        auth_client.oauth2_start_flow(
            requested_scopes=TRANSFER_SCOPE, refresh_tokens=True
        )
        print(f"\nPlease log in to Globus:\n{auth_client.oauth2_get_authorize_url()}\n")
        auth_code = input("Paste the authorisation code here: ").strip()
        tokens = auth_client.oauth2_exchange_code_for_tokens(auth_code)
        transfer_tokens = tokens.by_resource_server["transfer.api.globus.org"]
        self._save_tokens(transfer_tokens)
        return globus_sdk.RefreshTokenAuthorizer(
            transfer_tokens["refresh_token"],
            auth_client,
            access_token=transfer_tokens["access_token"],
            expires_at=transfer_tokens.get("expires_at_seconds"),
            on_refresh=self._save_tokens,
        )

    def _save_tokens(self, token_data) -> None:
        # token_data may be a dict or a OAuthTokenResponse object
        if hasattr(token_data, "data"):
            data = token_data.data
        elif hasattr(token_data, "__getitem__"):
            data = {
                "access_token": token_data["access_token"],
                "refresh_token": token_data["refresh_token"],
                "expires_at_seconds": token_data.get("expires_at_seconds"),
            }
        else:
            data = token_data
        TOKEN_CACHE.write_text(json.dumps(data))

    def _load_tokens(self) -> dict | None:
        if TOKEN_CACHE.exists():
            try:
                return json.loads(TOKEN_CACHE.read_text())
            except (json.JSONDecodeError, KeyError):
                pass
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_files(self, remote_path: str) -> list[str]:
        """Return a sorted list of filenames (not full paths) in remote_path."""
        result = self._transfer_client.operation_ls(
            self.source_endpoint, path=remote_path
        )
        return sorted(
            entry["name"]
            for entry in result
            if entry["type"] == "file"
        )

    def sample_files(self, remote_path: str, n: int, seed: int | None = None) -> list[str]:
        """Return n randomly sampled filenames from remote_path."""
        all_files = self.list_files(remote_path)
        if n > len(all_files):
            raise ValueError(
                f"Requested {n} files but only {len(all_files)} available in {remote_path}"
            )
        rng = random.Random(seed)
        return rng.sample(all_files, n)

    def download(self, filenames: list[str], remote_path: str) -> list[str]:
        """
        Transfer filenames from source_endpoint:remote_path to local_endpoint:local_dir.
        Files that already exist locally are skipped.
        Blocks until the transfer completes.
        Returns the list of local file paths.
        """
        to_fetch = [n for n in filenames if not (self.local_dir / n).exists()]
        already_have = len(filenames) - len(to_fetch)
        if already_have:
            print(f"Skipping {already_have} file(s) already present in {self.local_dir}")

        if to_fetch:
            task_data = globus_sdk.TransferData(
                source_endpoint=self.source_endpoint,
                destination_endpoint=self.local_endpoint,
            )
            for name in to_fetch:
                src = os.path.join(remote_path, name)
                dst = str(self.local_dir / name)
                task_data.add_item(src, dst)

            task = self._transfer_client.submit_transfer(task_data)
            task_id = task["task_id"]
            print(f"Submitted Globus transfer task {task_id} ({len(to_fetch)} file(s)) …")

            self._transfer_client.task_wait(
                task_id,
                polling_interval=10,
                timeout=3600,
            )

            status = self._transfer_client.get_task(task_id)["status"]
            if status != "SUCCEEDED":
                raise RuntimeError(f"Globus transfer task {task_id} finished with status: {status}")

            print(f"Transfer {task_id} completed successfully.")

        return [str(self.local_dir / name) for name in filenames]
