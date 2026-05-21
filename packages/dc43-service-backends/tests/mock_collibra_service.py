from __future__ import annotations

import email
from email.message import Message
import json
import uuid
import yaml
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import httpx

class MockCollibraService:
    """Mock implementation of Collibra REST APIs.
    
    Supports:
    - Core API REST 2.0 (assets, relations)
    - Search API REST 2.0 (search)
    - Data Product API v1 (dataContracts, versions, activeVersion, manifest downloads)
    """

    def __init__(self) -> None:
        # State stores
        self.assets: Dict[str, Dict[str, Any]] = {}
        self.relations: List[Dict[str, Any]] = []
        self.contracts: Dict[str, Dict[str, Any]] = {}
        self.contract_versions: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.contract_manifests: Dict[str, Dict[str, str]] = {}  # contract_uuid -> {version: yaml_str}

        # Default configuration UUIDs (matching those in our cascading lookup)
        self.relation_type_contains_id = "rel-contains-uuid-1111"
        self.relation_type_governed_id = "rel-governed-uuid-2222"
        self.data_product_type_id = "dp-type-uuid-3333"

    def clear(self) -> None:
        self.assets.clear()
        self.relations.clear()
        self.contracts.clear()
        self.contract_versions.clear()
        self.contract_manifests.clear()

    def add_asset(self, id: str, name: str, type_name: str, type_id: Optional[str] = None) -> None:
        self.assets[id] = {
            "id": id,
            "name": name,
            "displayName": name,
            "type": {
                "id": type_id or str(uuid.uuid4()),
                "name": type_name,
            }
        }

    def add_relation(self, source_id: str, target_id: str, relation_type_name: str, relation_type_id: str) -> None:
        self.relations.append({
            "id": str(uuid.uuid4()),
            "source": {
                "id": source_id,
                "name": self.assets.get(source_id, {}).get("name", "Unknown Source")
            },
            "target": {
                "id": target_id,
                "name": self.assets.get(target_id, {}).get("name", "Unknown Target")
            },
            "type": {
                "id": relation_type_id,
                "name": relation_type_name
            }
        })

    def _parse_multipart(self, request: httpx.Request) -> Dict[str, Any]:
        content_type = request.headers.get("Content-Type", "")
        if not content_type or "multipart/form-data" not in content_type:
            return {}
        
        # Parse standard email/MIME structure
        mime_data = b"Content-Type: " + content_type.encode("utf-8") + b"\r\n\r\n" + request.content
        msg = email.message_from_bytes(mime_data)
        
        parts: Dict[str, Any] = {}
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                disposition = part.get("Content-Disposition", "")
                if not disposition:
                    continue
                
                name = None
                for param in disposition.split(";"):
                    param = param.strip()
                    if param.startswith("name="):
                        name = param.split("=")[1].strip('"')
                        break
                
                if name:
                    filename = part.get_filename()
                    payload = part.get_payload(decode=True)
                    if filename:
                        parts[name] = {"filename": filename, "content": payload}
                    else:
                        parts[name] = payload.decode("utf-8") if payload else ""
        return parts

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        path = url.path
        method = request.method
        query_params = dict(url.params)

        # -------------------------------------------------------------
        # 1. CORE API REST 2.0 endpoints
        # -------------------------------------------------------------
        if path == "/rest/2.0/assets":
            if method == "GET":
                name_filter = query_params.get("name")
                type_id_filter = query_params.get("typeId")
                
                results = list(self.assets.values())
                if name_filter:
                    results = [a for a in results if a["name"] == name_filter or a["displayName"] == name_filter]
                if type_id_filter:
                    results = [a for a in results if a["type"]["id"] == type_id_filter]
                
                return httpx.Response(200, json={"total": len(results), "limit": 100, "results": results})

        elif path == "/rest/2.0/relations":
            if method == "GET":
                source_id = query_params.get("sourceId")
                target_id = query_params.get("targetId")
                relation_type_id = query_params.get("relationTypeId")

                results = self.relations
                if source_id:
                    results = [r for r in results if r["source"]["id"] == source_id]
                if target_id:
                    results = [r for r in results if r["target"]["id"] == target_id]
                if relation_type_id:
                    results = [r for r in results if r["type"]["id"] == relation_type_id]

                return httpx.Response(200, json={"total": len(results), "limit": 100, "results": results})

        # -------------------------------------------------------------
        # 2. SEARCH API REST 2.0 endpoints
        # -------------------------------------------------------------
        elif path == "/rest/2.0/search":
            if method == "POST":
                search_payload = json.loads(request.content.decode("utf-8"))
                keyword = search_payload.get("keyword", "")
                
                results = []
                for asset in self.assets.values():
                    if keyword in asset["name"] or keyword in asset["displayName"]:
                        results.append({
                            "id": asset["id"],
                            "name": asset["name"],
                            "type": asset["type"]["name"]
                        })
                return httpx.Response(200, json={"total": len(results), "results": results})

        # -------------------------------------------------------------
        # 3. DATA PRODUCT API V1 endpoints
        # -------------------------------------------------------------
        # GET /rest/dataProduct/v1/dataContracts
        elif path == "/rest/dataProduct/v1/dataContracts":
            if method == "GET":
                manifest_id = query_params.get("manifestId")
                results = list(self.contracts.values())
                if manifest_id:
                    results = [c for c in results if c["manifestId"] == manifest_id]
                
                return httpx.Response(200, json={
                    "total": len(results),
                    "limit": 100,
                    "nextCursor": None,
                    "items": results
                })

            # POST /rest/dataProduct/v1/dataContracts (Initialize)
            elif method == "POST":
                parts = self._parse_multipart(request)
                governed_asset_id = parts.get("governedAssetId")
                
                if not governed_asset_id:
                    return httpx.Response(400, json={
                        "statusCode": 400,
                        "userMessage": "Missing governedAssetId",
                        "errorCode": "invalidRequest"
                    })

                manifest_part = parts.get("manifest")
                if not manifest_part or not isinstance(manifest_part, dict):
                    return httpx.Response(400, json={
                        "statusCode": 400,
                        "userMessage": "Missing manifest file",
                        "errorCode": "invalidRequest"
                    })

                yaml_content = manifest_part["content"].decode("utf-8")
                manifest_dict = yaml.safe_load(yaml_content)

                manifest_id = parts.get("manifestId") or manifest_dict.get("id") or str(uuid.uuid4())
                version = parts.get("version") or manifest_dict.get("version") or "0.0.1"
                name = parts.get("name") or manifest_dict.get("name") or "Mock Contract"
                domain_id = parts.get("domainId") or str(uuid.uuid4())

                # Generate asset UUID
                contract_uuid = str(uuid.uuid4())

                # Store contract metadata
                self.contracts[contract_uuid] = {
                    "id": contract_uuid,
                    "manifestId": manifest_id,
                    "name": name,
                    "domainId": domain_id,
                    "domainName": "Mock Domain",
                    "activeVersion": version
                }

                # Store version metadata
                version_info = {
                    "version": version,
                    "active": True,
                    "format": "ODCS",
                    "createdBy": "00000000-0000-0000-0000-000000000001",
                    "createdOn": int(datetime.utcnow().timestamp() * 1000),
                    "lastModifiedBy": "00000000-0000-0000-0000-000000000001",
                    "lastModifiedOn": int(datetime.utcnow().timestamp() * 1000)
                }

                self.contract_versions[contract_uuid] = {version: version_info}
                self.contract_manifests.setdefault(contract_uuid, {})[version] = yaml_content

                # Create graph relation: Port governed by Data Contract
                self.add_asset(contract_uuid, name, "Data Contract")
                self.add_relation(
                    source_id=contract_uuid,
                    target_id=governed_asset_id,
                    relation_type_name="Port governed by Data Contract",
                    relation_type_id=self.relation_type_governed_id
                )

                response_body = {
                    **self.contracts[contract_uuid],
                    "manifestVersion": version_info
                }
                return httpx.Response(201, json=response_body)

        # GET /rest/dataProduct/v1/dataContracts/{id}
        elif path.startswith("/rest/dataProduct/v1/dataContracts/"):
            subpath = path[len("/rest/dataProduct/v1/dataContracts/"):]
            parts_path = subpath.split("/")
            contract_uuid = parts_path[0]

            if contract_uuid not in self.contracts:
                return httpx.Response(404, json={
                    "statusCode": 404,
                    "userMessage": "Contract not found",
                    "errorCode": "notFound"
                })

            # GET /rest/dataProduct/v1/dataContracts/{id} (Metadata)
            if len(parts_path) == 1:
                if method == "GET":
                    return httpx.Response(200, json=self.contracts[contract_uuid])

            # GET or POST versions
            elif parts_path[1] == "versions":
                # GET /rest/dataProduct/v1/dataContracts/{id}/versions/manifest
                if len(parts_path) == 3 and parts_path[2] == "manifest":
                    if method == "GET":
                        version = query_params.get("version")
                        if not version:
                            return httpx.Response(400, json={"statusCode": 400, "userMessage": "Missing version"})
                        manifests = self.contract_manifests.get(contract_uuid, {})
                        if version not in manifests:
                            return httpx.Response(404, json={"statusCode": 404, "userMessage": "Version manifest not found"})
                        
                        return httpx.Response(
                            200,
                            content=manifests[version].encode("utf-8"),
                            headers={"Content-Type": "application/yaml"}
                        )

                # GET or POST versions
                elif len(parts_path) == 2:
                    # GET /rest/dataProduct/v1/dataContracts/{id}/versions
                    if method == "GET":
                        versions = list(self.contract_versions.get(contract_uuid, {}).values())
                        # Order active=true first
                        versions.sort(key=lambda v: (not v["active"], v["version"]))
                        return httpx.Response(200, json={
                            "total": len(versions),
                            "limit": 100,
                            "nextCursor": None,
                            "items": versions
                        })
                    
                    # POST /rest/dataProduct/v1/dataContracts/{id}/versions (Upload)
                    elif method == "POST":
                        parts = self._parse_multipart(request)
                        manifest_part = parts.get("manifest")
                        if not manifest_part or not isinstance(manifest_part, dict):
                            return httpx.Response(400, json={"statusCode": 400, "userMessage": "Missing manifest"})

                        yaml_content = manifest_part["content"].decode("utf-8")
                        manifest_dict = yaml.safe_load(yaml_content)

                        version = parts.get("version") or manifest_dict.get("version") or "0.0.1"
                        active = parts.get("active") != "false" # Defaults to True
                        
                        version_info = {
                            "version": version,
                            "active": active,
                            "format": "ODCS",
                            "createdBy": "00000000-0000-0000-0000-000000000001",
                            "createdOn": int(datetime.utcnow().timestamp() * 1000),
                            "lastModifiedBy": "00000000-0000-0000-0000-000000000001",
                            "lastModifiedOn": int(datetime.utcnow().timestamp() * 1000)
                        }

                        if active:
                            # Deactivate others
                            for v_name, v_info in self.contract_versions.get(contract_uuid, {}).items():
                                v_info["active"] = False
                            self.contracts[contract_uuid]["activeVersion"] = version

                        self.contract_versions.setdefault(contract_uuid, {})[version] = version_info
                        self.contract_manifests.setdefault(contract_uuid, {})[version] = yaml_content

                        response_body = {
                            **self.contracts[contract_uuid],
                            "manifestVersion": version_info
                        }
                        return httpx.Response(201, json=response_body)

            # GET or PATCH activeVersion
            elif parts_path[1] == "activeVersion":
                # GET /rest/dataProduct/v1/dataContracts/{id}/activeVersion/manifest
                if len(parts_path) == 3 and parts_path[2] == "manifest":
                    if method == "GET":
                        active_version = self.contracts[contract_uuid]["activeVersion"]
                        yaml_content = self.contract_manifests.get(contract_uuid, {}).get(active_version)
                        if not yaml_content:
                            return httpx.Response(404, json={"statusCode": 404, "userMessage": "Active manifest not found"})
                        
                        return httpx.Response(
                            200,
                            content=yaml_content.encode("utf-8"),
                            headers={"Content-Type": "application/yaml"}
                        )

                elif len(parts_path) == 2:
                    # GET /rest/dataProduct/v1/dataContracts/{id}/activeVersion
                    if method == "GET":
                        active_version = self.contracts[contract_uuid]["activeVersion"]
                        v_info = self.contract_versions.get(contract_uuid, {}).get(active_version)
                        if not v_info:
                            return httpx.Response(404, json={"statusCode": 404, "userMessage": "Active version info not found"})
                        return httpx.Response(200, json=v_info)

                    # PATCH /rest/dataProduct/v1/dataContracts/{id}/activeVersion
                    elif method == "PATCH":
                        target_version = query_params.get("version")
                        if not target_version:
                            return httpx.Response(400, json={"statusCode": 400, "userMessage": "Missing version query param"})
                        
                        versions_dict = self.contract_versions.get(contract_uuid, {})
                        if target_version not in versions_dict:
                            return httpx.Response(404, json={"statusCode": 404, "userMessage": f"Version {target_version} not found"})

                        for v_name, v_info in versions_dict.items():
                            v_info["active"] = (v_name == target_version)
                        
                        self.contracts[contract_uuid]["activeVersion"] = target_version
                        return httpx.Response(200, json=versions_dict[target_version])

        return httpx.Response(404, json={"statusCode": 404, "userMessage": f"Not found path {path} or method {method}"})

    def get_transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle_request)
