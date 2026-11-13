#!/usr/bin/env python3
"""One-time-per-file Google Drive delivery for Stage3DS Fog.

This is create-only: it never edits or replaces a Drive file. Successful uploads
are recorded immediately in .collector_manifest.json. appProperties provide an
idempotency guard if a run stops after Drive accepts an upload but before the
manifest write completes.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

FOLDER = Path("stage3dsfog")
MANIFEST = FOLDER / ".collector_manifest.json"
TOKEN_URL = "https://oauth2.googleapis.com/token"
DRIVE_API = "https://www.googleapis.com/drive/v3"
UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"


def request_json(url, data=None, headers=None, method=None):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=180) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8")) if raw else {}


def save_manifest(manifest):
    tmp = MANIFEST.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(MANIFEST)


def get_access_token():
    required = ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET", "GDRIVE_REFRESH_TOKEN")
    missing = [key for key in required if not os.getenv(key, "").strip()]
    if missing:
        raise RuntimeError("Missing GitHub Actions secrets: " + ", ".join(missing))
    payload = urllib.parse.urlencode({
        "client_id": os.environ["GDRIVE_CLIENT_ID"],
        "client_secret": os.environ["GDRIVE_CLIENT_SECRET"],
        "refresh_token": os.environ["GDRIVE_REFRESH_TOKEN"],
        "grant_type": "refresh_token",
    }).encode()
    result = request_json(TOKEN_URL, payload, {"Content-Type": "application/x-www-form-urlencoded"})
    token = result.get("access_token")
    if not token:
        raise RuntimeError("Google OAuth did not return an access token.")
    return token


def drive_request(url, token, data=None, headers=None, method=None):
    h = {"Authorization": "Bearer " + token}
    h.update(headers or {})
    return request_json(url, data, h, method)


def find_previously_created(name, token, folder_id):
    # This is an idempotency lookup only. No file content is downloaded,
    # compared, patched, or replaced.
    escaped = name.replace("\\", "\\\\").replace("'", "\\'")
    query = "'" + folder_id + "' in parents and appProperties has { key='stage3dsfog_name' and value='" + escaped + "' } and trashed=false"
    params = urllib.parse.urlencode({
        "q": query,
        "pageSize": "10",
        "fields": "files(id,name)",
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    })
    result = drive_request(DRIVE_API + "/files?" + params, token)
    matches = result.get("files", [])
    return matches[0] if matches else None


def create_drive_file(path, token, folder_id):
    boundary = "stage3dsfog_" + uuid.uuid4().hex
    metadata = {
        "name": path.name,
        "mimeType": "application/json",
        "parents": [folder_id],
        "appProperties": {"stage3dsfog_name": path.name},
    }
    body = bytearray()
    body.extend(("--" + boundary + "\r\n").encode())
    body.extend(b"Content-Type: application/json; charset=UTF-8\r\n\r\n")
    body.extend(json.dumps(metadata, ensure_ascii=False).encode("utf-8"))
    body.extend(("\r\n--" + boundary + "\r\n").encode())
    body.extend(b"Content-Type: application/json\r\n\r\n")
    body.extend(path.read_bytes())
    body.extend(("\r\n--" + boundary + "--\r\n").encode())

    params = urllib.parse.urlencode({
        "uploadType": "multipart",
        "supportsAllDrives": "true",
        "fields": "id,name",
    })
    return drive_request(
        UPLOAD_API + "?" + params,
        token,
        bytes(body),
        {"Content-Type": "multipart/related; boundary=" + boundary},
        "POST",
    )


def main():
    folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        raise RuntimeError("Missing GitHub Actions secret: GDRIVE_FOLDER_ID")

    FOLDER.mkdir(parents=True, exist_ok=True)
    if MANIFEST.exists():
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    else:
        manifest = {"version": 1, "files": {}}
    manifest.setdefault("version", 1)
    manifest.setdefault("files", {})

    # Backfill every existing JSON in the collector folder, including files
    # created before Drive delivery was added.
    for path in sorted(FOLDER.glob("*.json")):
        if path.name == MANIFEST.name:
            continue
        manifest["files"].setdefault(path.name, {
            "collected_at": "existing-file-backfill",
            "telegram_sent": False,
        })
    save_manifest(manifest)

    pending = [
        name for name, info in manifest["files"].items()
        if not info.get("drive_uploaded", False)
    ]
    print(f"Pending Google Drive uploads (including backfill): {len(pending)}")
    if not pending:
        return

    token = get_access_token()
    failed = []
    for name in pending:
        path = FOLDER / name
        if not path.is_file() or path.suffix.lower() != ".json":
            print(f"Skipping missing/non-JSON collector entry: {name}")
            failed.append(name)
            continue

        try:
            existing = find_previously_created(name, token, folder_id)
            if existing:
                result = existing
                print(f"Drive already has this collector file; recording it without re-upload: {name}")
            else:
                result = create_drive_file(path, token, folder_id)
                print(f"Uploaded to Google Drive: {name}")

            # Mark success immediately. A successful file is never uploaded
            # again and is never compared or updated by this workflow.
            info = manifest["files"][name]
            info["drive_uploaded"] = True
            info["drive_file_id"] = result["id"]
            info["drive_uploaded_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            save_manifest(manifest)
        except Exception as exc:
            print(f"Google Drive upload failed for {name}: {exc}")
            failed.append(name)

    print(f"Google Drive delivery finished. Failed/pending: {len(failed)}")
    if failed:
        print("Only unsuccessful uploads remain pending for a future run:")
        for name in failed:
            print("  - " + name)


if __name__ == "__main__":
    main()
