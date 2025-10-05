"""Export stage that packages final campaign assets."""
from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import mimetypes
import uuid
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from shared.config import get_settings
from shared.models import AssetRecord
from shared.stages.base import BaseStage
from shared.storage import SignedUpload, upload_bytes


class ExportStage(BaseStage):
    name = "export"

    def execute(self) -> Dict[str, Any]:
        self.ensure_budget(20.0)
        session = self.context.session
        run = self.context.run
        run_id_str = str(run.id)
        settings = get_settings()
        generated_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)

        output_root = self._resolve_output_root(run_id_str)
        csv_paths = self._gather_csvs(output_root)
        image_paths = self._gather_images(output_root)
        qa_paths = self._gather_qa_reports(output_root, run_id_str)

        collected_assets: List[Tuple[str, Path]] = (
            [("csvs", path) for path in csv_paths]
            + [("images", path) for path in image_paths]
            + [("qa_reports", path) for path in qa_paths]
        )

        assets_metadata = {
            "csvs": [self._file_metadata(path, "csvs") for path in csv_paths],
            "images": [self._file_metadata(path, "images") for path in image_paths],
            "qa_reports": [self._file_metadata(path, "qa_reports") for path in qa_paths],
        }
        asset_counts = {category: len(items) for category, items in assets_metadata.items()}
        asset_counts["total"] = sum(asset_counts.values())

        optional_exports = self._gather_optional_exports(settings)
        readme_entries = self._build_readme_map(assets_metadata, optional_exports)
        readme_md = self._render_readme(run_id_str, generated_at, readme_entries)

        bundle_key = f"exports/{run_id_str}/bundle.zip"
        manifest_key = f"exports/{run_id_str}/manifest.json"
        manifest_document: Dict[str, Any] = {
            "run_id": run_id_str,
            "generated_at": generated_at.isoformat(),
            "output_root": str(output_root),
            "assets": assets_metadata,
            "readme_map": readme_entries,
            "optional_exports": optional_exports,
            "bundle": {
                "storage_key": bundle_key,
                "size_bytes": 0,
                "sha256": "",
            },
            "manifest": {
                "storage_key": manifest_key,
            },
            "counts": asset_counts,
        }

        manifest_bytes, bundle_bytes = self._materialize_bundle(
            collected_assets,
            manifest_document,
            readme_md,
            output_root,
        )
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        manifest_size = len(manifest_bytes)
        bundle_size = len(bundle_bytes)
        bundle_hash = hashlib.sha256(bundle_bytes).hexdigest()

        manifest_document["bundle"]["size_bytes"] = bundle_size
        manifest_document["bundle"]["sha256"] = bundle_hash

        ttl = settings.export_bundle_ttl_seconds
        manifest_upload: SignedUpload | None = None
        manifest_error: str | None = None
        bundle_upload: SignedUpload | None = None
        bundle_error: str | None = None

        try:
            manifest_upload = upload_bytes(
                manifest_key,
                manifest_bytes,
                content_type="application/json",
                ttl_seconds=ttl,
            )
        except Exception as exc:  # pragma: no cover - optional dependency path
            manifest_error = str(exc)

        try:
            bundle_upload = upload_bytes(
                bundle_key,
                bundle_bytes,
                content_type="application/zip",
                ttl_seconds=ttl,
            )
        except Exception as exc:  # pragma: no cover - optional dependency path
            bundle_error = str(exc)

        manifest_record = self._persist_asset_record(
            run_id=run.id,
            asset_type="manifest",
            storage_key=manifest_upload.key if manifest_upload else f"minio-unavailable://{manifest_error}",
            extra={
                "size_bytes": manifest_size,
                "sha256": manifest_hash,
                "content_type": "application/json",
                **(
                    {
                        "signed_url": manifest_upload.signed_url,
                        "expires_at": manifest_upload.expires_at.isoformat(),
                        "ttl_seconds": ttl,
                    }
                    if manifest_upload
                    else {"error": manifest_error or "upload failed"}
                ),
            },
        )

        bundle_record = self._persist_asset_record(
            run_id=run.id,
            asset_type="export_bundle",
            storage_key=bundle_upload.key if bundle_upload else f"minio-unavailable://{bundle_error}",
            extra={
                "size_bytes": bundle_size,
                "sha256": bundle_hash,
                "content_type": "application/zip",
                "asset_counts": asset_counts,
                **(
                    {
                        "signed_url": bundle_upload.signed_url,
                        "expires_at": bundle_upload.expires_at.isoformat(),
                        "ttl_seconds": ttl,
                    }
                    if bundle_upload
                    else {"error": bundle_error or "upload failed"}
                ),
            },
        )

        session.add(manifest_record)
        session.add(bundle_record)
        session.commit()

        telemetry: Dict[str, Any] = {
            "manifest": {
                "storage_key": manifest_record.storage_key,
                "size_bytes": manifest_size,
                "sha256": manifest_hash,
            },
            "bundle": {
                "storage_key": bundle_record.storage_key,
                "size_bytes": bundle_size,
                "sha256": bundle_hash,
            },
            "asset_counts": asset_counts,
            "assets": assets_metadata,
            "readme_map": readme_entries,
            "optional_exports": optional_exports,
            "manifest_document": manifest_document,
        }
        if manifest_upload:
            telemetry["manifest"].update(
                {
                    "signed_url": manifest_upload.signed_url,
                    "expires_at": manifest_upload.expires_at.isoformat(),
                    "ttl_seconds": ttl,
                }
            )
        else:
            telemetry["manifest"]["error"] = manifest_error or "upload failed"
        if bundle_upload:
            telemetry["bundle"].update(
                {
                    "signed_url": bundle_upload.signed_url,
                    "expires_at": bundle_upload.expires_at.isoformat(),
                    "ttl_seconds": ttl,
                }
            )
        else:
            telemetry["bundle"]["error"] = bundle_error or "upload failed"

        telemetry["export_package"] = (
            telemetry["bundle"].get("signed_url")
            or telemetry["bundle"].get("storage_key")
        )

        return telemetry

    # Helpers -----------------------------------------------------------------

    def _resolve_output_root(self, run_id: str) -> Path:
        candidate = Path("outputs") / run_id
        if candidate.exists():
            return candidate
        default = Path("outputs")
        if default.exists():
            return default
        return Path(".")

    def _gather_csvs(self, output_root: Path) -> List[Path]:
        directories = [
            output_root / "audiences",
            output_root / "creatives",
            output_root,
        ]
        return self._gather_files(directories, ["**/*.csv"])

    def _gather_images(self, output_root: Path) -> List[Path]:
        directories = [
            output_root / "creatives" / "images",
            output_root / "images",
        ]
        patterns = ["**/*.png", "**/*.jpg", "**/*.jpeg", "**/*.webp"]
        return self._gather_files(directories, patterns)

    def _gather_qa_reports(self, output_root: Path, run_id: str) -> List[Path]:
        directories = [
            output_root / "qa_reports",
            Path("qa_reports") / run_id,
            Path("qa_reports"),
            output_root / "qa" / "reports",
        ]
        return self._gather_files(directories, ["**/*"])  # filtered in metadata helper

    def _gather_files(self, directories: Iterable[Path], patterns: Iterable[str]) -> List[Path]:
        seen: set[Path] = set()
        results: List[Path] = []
        for directory in directories:
            if not directory.exists():
                continue
            for pattern in patterns:
                for path in directory.glob(pattern):
                    if path.is_file():
                        resolved = path.resolve()
                        if resolved in seen:
                            continue
                        seen.add(resolved)
                        results.append(path)
        results.sort()
        return results

    def _file_metadata(self, path: Path, category: str) -> Dict[str, Any]:
        stat = path.stat()
        try:
            relative = path.relative_to(Path.cwd())
        except ValueError:
            relative = path
        relative_str = str(relative)
        mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        description = self._describe_asset(path, category)
        return {
            "path": relative_str,
            "category": category,
            "size_bytes": stat.st_size,
            "modified_at": dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc).isoformat(),
            "mime_type": mime_type,
            "description": description,
        }

    def _describe_asset(self, path: Path, category: str) -> str:
        name = path.name.lower()
        if "audiences" in name and path.suffix.lower() == ".csv":
            return "Audience matrix CSV export"
        if "scroll" in name and path.suffix.lower() == ".csv":
            return "Scroll stoppers creative CSV export"
        if category == "images":
            return "Rendered creative image"
        if category == "qa_reports":
            if path.suffix.lower() in {".html", ".htm"}:
                return "QA report (HTML)"
            if path.suffix.lower() in {".json", ".csv"}:
                return "QA diagnostic artifact"
            return "QA report artifact"
        return f"{category[:-1].capitalize()} asset"

    def _build_readme_map(
        self,
        assets_metadata: Dict[str, List[Dict[str, Any]]],
        optional_exports: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for category, items in assets_metadata.items():
            for item in items:
                path = Path(item["path"])
                title = self._title_for_entry(path, category)
                entries.append(
                    {
                        "title": title,
                        "category": category,
                        "path": item["path"],
                        "description": item["description"],
                    }
                )
        google_cfg = optional_exports.get("google_sheets")
        if isinstance(google_cfg, dict) and google_cfg.get("workbook_url"):
            entries.append(
                {
                    "title": "Google Sheets Export",
                    "category": "external",
                    "url": google_cfg.get("workbook_url"),
                    "description": "Live Google Sheets workbook mirroring audiences_master and scroll_stoppers tabs.",
                }
            )
        meta_cfg = optional_exports.get("meta")
        if isinstance(meta_cfg, dict):
            meta_url = meta_cfg.get("download_url") or meta_cfg.get("asset_url")
            if meta_url:
                entries.append(
                    {
                        "title": "Meta Export",
                        "category": "external",
                        "url": meta_url,
                        "description": "Meta export template or asset library reference for campaign upload.",
                    }
                )
        return entries

    def _title_for_entry(self, path: Path, category: str) -> str:
        if category == "csvs":
            if "audiences" in path.name.lower():
                return "Audiences Master CSV"
            if "scroll" in path.name.lower():
                return "Scroll Stoppers CSV"
            return f"CSV Export ({path.name})"
        if category == "images":
            return f"Rendered Image ({path.name})"
        if category == "qa_reports":
            return f"QA Report ({path.name})"
        return f"{category.title()} ({path.name})"

    def _render_readme(
        self,
        run_id: str,
        generated_at: dt.datetime,
        entries: List[Dict[str, Any]],
    ) -> str:
        lines = [
            "# Andronoma Export Bundle",
            "",
            f"- **Run ID:** {run_id}",
            f"- **Generated:** {generated_at.isoformat()}",
            "",
            "## Asset Inventory",
        ]
        for entry in entries:
            location = entry.get("url") or entry.get("path")
            description = entry.get("description") or ""
            lines.append(f"- **{entry['title']}** ({entry['category']}): {description}")
            if location:
                lines.append(f"  - Location: {location}")
        return "\n".join(lines) + "\n"

    def _gather_optional_exports(self, settings: Any) -> Dict[str, Any]:
        optional: Dict[str, Any] = {}
        run_telemetry = self.context.run.telemetry or {}
        run_optional = run_telemetry.get("optional_exports")
        if isinstance(run_optional, dict):
            optional.update(run_optional)
        google_url = settings.google_sheets_export_url
        if google_url:
            google_entry = dict(optional.get("google_sheets", {}))
            google_entry.setdefault("workbook_url", google_url)
            google_entry.setdefault("tabs", ["audiences_master", "scroll_stoppers"])
            optional["google_sheets"] = google_entry
        meta_url = settings.meta_export_url
        if meta_url:
            meta_entry = dict(optional.get("meta", {}))
            meta_entry.setdefault("download_url", meta_url)
            optional["meta"] = meta_entry
        return optional

    def _materialize_bundle(
        self,
        assets: List[Tuple[str, Path]],
        manifest_document: Dict[str, Any],
        readme_md: str,
        output_root: Path,
    ) -> Tuple[bytes, bytes]:
        manifest_document["bundle"].setdefault("size_bytes", 0)
        manifest_document["bundle"].setdefault("sha256", "")
        manifest_bytes = b""
        bundle_bytes = b""
        for _ in range(5):
            manifest_bytes = json.dumps(manifest_document, indent=2, sort_keys=True).encode("utf-8")
            bundle_bytes = self._build_bundle_bytes(assets, manifest_bytes, readme_md, output_root)
            bundle_hash = hashlib.sha256(bundle_bytes).hexdigest()
            bundle_size = len(bundle_bytes)
            if (
                bundle_size == manifest_document["bundle"].get("size_bytes")
                and bundle_hash == manifest_document["bundle"].get("sha256")
            ):
                break
            manifest_document["bundle"]["size_bytes"] = bundle_size
            manifest_document["bundle"]["sha256"] = bundle_hash
        return manifest_bytes, bundle_bytes

    def _build_bundle_bytes(
        self,
        assets: List[Tuple[str, Path]],
        manifest_bytes: bytes,
        readme_md: str,
        output_root: Path,
    ) -> bytes:
        buffer = io.BytesIO()
        seen: set[str] = set()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for category, path in assets:
                if not path.exists() or not path.is_file():
                    continue
                arcname = self._archive_name(category, path, output_root)
                if arcname in seen:
                    continue
                archive.write(path, arcname)
                seen.add(arcname)
            archive.writestr("manifest.json", manifest_bytes)
            archive.writestr("README_MAP.md", readme_md)
        return buffer.getvalue()

    def _archive_name(self, category: str, path: Path, output_root: Path) -> str:
        try:
            relative = path.relative_to(output_root)
        except ValueError:
            try:
                relative = path.relative_to(Path.cwd())
            except ValueError:
                relative = path.name
        else:
            relative = Path(category) / relative
            return str(relative)
        return str(Path(category) / Path(relative))

    def _persist_asset_record(
        self,
        run_id,
        asset_type: str,
        storage_key: str,
        extra: Dict[str, Any],
    ) -> AssetRecord:
        record = AssetRecord(
            id=uuid.uuid4(),
            run_id=run_id,
            stage=self.name,
            asset_type=asset_type,
            storage_key=storage_key,
            extra=extra,
        )
        return record
