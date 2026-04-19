import os
import subprocess
import uuid

import boto3
from botocore.client import Config
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"),
)

BUCKET = os.environ["R2_BUCKET_NAME"]
PUBLIC_URL = os.environ["R2_PUBLIC_URL"]


class DownloadRequest(BaseModel):
    url: str


class CleanupRequest(BaseModel):
    keys: list[str]


def public_asset_url(key: str) -> str:
    return f"{PUBLIC_URL.rstrip('/')}/{key}"


def delete_objects(keys: list[str]) -> None:
    cleaned_keys = [key for key in keys if key]

    if not cleaned_keys:
        return

    s3.delete_objects(
        Bucket=BUCKET,
        Delete={"Objects": [{"Key": key} for key in cleaned_keys], "Quiet": True},
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/download")
def download(req: DownloadRequest):
    file_id = str(uuid.uuid4())
    video_tmp_path = f"/tmp/{file_id}.mp4"
    thumbnail_tmp_path = f"/tmp/{file_id}.jpg"
    video_key = f"videos/{file_id}.mp4"
    thumbnail_key = f"thumbnails/{file_id}.jpg"
    uploaded_keys: list[str] = []

    try:
        download_result = subprocess.run(
            [
                "yt-dlp",
                "--no-playlist",
                "--merge-output-format",
                "mp4",
                "-o",
                video_tmp_path,
                req.url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if download_result.returncode != 0:
            raise HTTPException(status_code=400, detail=f"yt-dlp error: {download_result.stderr}")

        thumbnail_result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                video_tmp_path,
                "-frames:v",
                "1",
                "-q:v",
                "2",
                thumbnail_tmp_path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if thumbnail_result.returncode != 0:
            raise HTTPException(status_code=500, detail="Failed to generate thumbnail.")

        s3.upload_file(
            video_tmp_path,
            BUCKET,
            video_key,
            ExtraArgs={"ContentType": "video/mp4"},
        )
        uploaded_keys.append(video_key)

        s3.upload_file(
            thumbnail_tmp_path,
            BUCKET,
            thumbnail_key,
            ExtraArgs={"ContentType": "image/jpeg"},
        )
        uploaded_keys.append(thumbnail_key)

        return {
            "video_url": public_asset_url(video_key),
            "video_key": video_key,
            "thumbnail_url": public_asset_url(thumbnail_key),
            "thumbnail_key": thumbnail_key,
        }
    except subprocess.TimeoutExpired:
        delete_objects(uploaded_keys)
        raise HTTPException(status_code=408, detail="Download timed out")
    except HTTPException:
        delete_objects(uploaded_keys)
        raise
    except Exception as error:
        delete_objects(uploaded_keys)
        raise HTTPException(status_code=500, detail=f"Unexpected downloader error: {error}") from error
    finally:
        for tmp_path in (video_tmp_path, thumbnail_tmp_path):
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


@app.post("/cleanup")
def cleanup(req: CleanupRequest):
    try:
        delete_objects(req.keys)
        return {"ok": True}
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Failed to clean up objects: {error}") from error
