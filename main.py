import os
import uuid
import subprocess
import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from botocore.client import Config

app = FastAPI()

# R2 client
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"),
)

BUCKET = os.environ["R2_BUCKET_NAME"]
PUBLIC_URL = os.environ["R2_PUBLIC_URL"]  # e.g. https://pub-xxxx.r2.dev


class DownloadRequest(BaseModel):
    url: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/download")
def download(req: DownloadRequest):
    file_id = str(uuid.uuid4())
    tmp_path = f"/tmp/{file_id}.mp4"

    try:
        # Download with yt-dlp
        result = subprocess.run(
            [
                "yt-dlp",
                "--no-playlist",
                "--merge-output-format", "mp4",
                "-o", tmp_path,
                req.url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            raise HTTPException(status_code=400, detail=f"yt-dlp error: {result.stderr}")

        # Upload to R2
        r2_key = f"videos/{file_id}.mp4"
        s3.upload_file(
            tmp_path,
            BUCKET,
            r2_key,
            ExtraArgs={"ContentType": "video/mp4"},
        )

        r2_url = f"{PUBLIC_URL}/{r2_key}"
        return {"r2_url": r2_url, "key": r2_key}

    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Download timed out")

    finally:
        # Always clean up temp file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
