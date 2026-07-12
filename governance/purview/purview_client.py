"""Shared Purview API client with authentication helpers.

Provides a configured client for interacting with Microsoft Purview APIs.
Supports both Azure CLI auth and service principal auth.

Usage:
    from purview_client import PurviewClient

    client = PurviewClient(account_name="my-purview-account")
    # or with service principal:
    client = PurviewClient(
        account_name="my-purview-account",
        tenant_id="...",
        client_id="...",
        client_secret="...",
    )
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests
from azure.identity import ClientSecretCredential, DefaultAzureCredential

logger = logging.getLogger(__name__)


class PurviewClient:
    """Client for Microsoft Purview REST APIs."""

    def __init__(
        self,
        account_name: str | None = None,
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
    ):
        self.account_name = account_name or os.environ.get("PURVIEW_ACCOUNT_NAME")
        if not self.account_name:
            raise ValueError(
                "Purview account name required. Set PURVIEW_ACCOUNT_NAME or pass account_name."
            )

        self.base_url = f"https://{self.account_name}.purview.azure.com"
        self.catalog_url = f"{self.base_url}/catalog/api"
        self.scan_url = f"{self.base_url}/scan/datasources"

        tenant_id = tenant_id or os.environ.get("PURVIEW_TENANT_ID")
        client_id = client_id or os.environ.get("PURVIEW_CLIENT_ID")
        client_secret = client_secret or os.environ.get("PURVIEW_CLIENT_SECRET")

        if client_id and client_secret and tenant_id:
            self._credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        else:
            self._credential = DefaultAzureCredential()

        self._token_scope = "https://purview.azure.net/.default"

    def _get_headers(self) -> dict[str, str]:
        token = self._credential.get_token(self._token_scope)
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

    def _request(
        self, method: str, url: str, body: dict | None = None, api_version: str = "2022-03-01-preview"
    ) -> dict[str, Any]:
        headers = self._get_headers()
        params = {"api-version": api_version}

        resp = requests.request(
            method, url, headers=headers, params=params,
            json=body, timeout=30,
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    # ── Entity Operations ──────────────────────────────────────────────

    def get_entity_by_qualified_name(self, type_name: str, qualified_name: str) -> dict | None:
        """Look up an entity by its qualified name."""
        url = f"{self.catalog_url}/atlas/v2/entity/uniqueAttribute/type/{type_name}"
        try:
            return self._request("GET", url + f"?attr:qualifiedName={qualified_name}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def create_or_update_entity(self, entity: dict) -> dict:
        """Create or update a Purview entity."""
        url = f"{self.catalog_url}/atlas/v2/entity"
        return self._request("POST", url, body={"entity": entity})

    def create_or_update_entities(self, entities: list[dict]) -> dict:
        """Bulk create or update entities."""
        url = f"{self.catalog_url}/atlas/v2/entity/bulk"
        return self._request("POST", url, body={"entities": entities})

    # ── Type Operations ────────────────────────────────────────────────

    def get_type_def(self, name: str) -> dict | None:
        """Get a type definition by name."""
        url = f"{self.catalog_url}/atlas/v2/types/typedef/name/{name}"
        try:
            return self._request("GET", url)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def create_or_update_type_defs(self, type_defs: dict) -> dict:
        """Create or update type definitions."""
        url = f"{self.catalog_url}/atlas/v2/types/typedefs"
        return self._request("POST", url, body=type_defs)

    # ── Lineage Operations ─────────────────────────────────────────────

    def create_lineage(self, process_entity: dict) -> dict:
        """Create a lineage relationship via a process entity."""
        return self.create_or_update_entity(process_entity)

    # ── Glossary Operations ────────────────────────────────────────────

    def get_glossary(self) -> dict:
        """Get the default glossary."""
        url = f"{self.catalog_url}/atlas/v2/glossary"
        return self._request("GET", url)

    def create_glossary_term(self, term: dict) -> dict:
        """Create a glossary term."""
        url = f"{self.catalog_url}/atlas/v2/glossary/term"
        return self._request("POST", url, body=term)

    # ── Collection Operations ──────────────────────────────────────────

    def create_collection(self, name: str, parent_name: str | None = None) -> dict:
        """Create a collection under the given parent."""
        url = f"{self.base_url}/account/collections/{name}"
        body: dict[str, Any] = {"friendlyName": name}
        if parent_name:
            body["parentCollection"] = {"referenceName": parent_name}
        return self._request("PUT", url, body=body)

    # ── Classification Operations ──────────────────────────────────────

    def classify_entity(self, entity_guid: str, classification_name: str) -> None:
        """Add a classification to an entity."""
        url = f"{self.catalog_url}/atlas/v2/entity/guid/{entity_guid}/classifications"
        body = [{"typeName": classification_name}]
        self._request("POST", url, body=body)
