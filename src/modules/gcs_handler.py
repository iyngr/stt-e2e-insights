"""Google Cloud Storage handler for STT E2E Insights."""

import asyncio
from typing import List, Optional, Dict, Any
from pathlib import Path
import tempfile
import os

from google.cloud import storage
from google.cloud.storage import Blob

from ..utils.logger import LoggerMixin
from ..utils.config_loader import get_config_section
from ..utils.async_helpers import sync_to_async, async_retry


class GCSHandler(LoggerMixin):
    """Handles Google Cloud Storage operations for audio files and processed data."""
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize the GCS handler.
        
        Args:
            project_id: GCP project ID. If None, loads from config.
        """
        try:
            gcp_config = get_config_section('gcp')
            gcs_config = get_config_section('gcs')
        except KeyError as e:
            raise ValueError(f"Missing configuration section: {e}")
        
        self.project_id = project_id or gcp_config.get('project_id')
        if not self.project_id:
            raise ValueError("GCP project ID must be provided")
        
        self.input_bucket_name = gcs_config['input_bucket']
        self.output_bucket_name = gcs_config['output_bucket']
        self.input_folder = gcs_config.get('input_folder', '')
        self.output_folder = gcs_config.get('output_folder', '')
        self.file_prefix_filter = gcs_config.get('file_prefix_filter', 'merged')
        
        # Initialize GCS client
        self.client = storage.Client(project=self.project_id)
        self.input_bucket = self.client.bucket(self.input_bucket_name)
        self.output_bucket = self.client.bucket(self.output_bucket_name)
        
        self.logger.info("GCS handler initialized",
                        project_id=self.project_id,
                        input_bucket=self.input_bucket_name,
                        output_bucket=self.output_bucket_name)
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def list_audio_files(self) -> List[str]:
        """List audio files in the input bucket that match the prefix filter.
        
        Returns:
            List of GCS blob names (file paths) that match the criteria.
        """
        self.logger.info("Listing audio files",
                        bucket=self.input_bucket_name,
                        folder=self.input_folder,
                        prefix_filter=self.file_prefix_filter)
        
        # Use sync_to_async to make the blocking operation async
        blobs = await sync_to_async(list)(
            self.input_bucket.list_blobs(prefix=self.input_folder)
        )
        
        # Filter blobs based on prefix and ensure they are audio files
        audio_extensions = {'.wav', '.mp3', '.flac', '.m4a', '.aac', '.ogg', '.au', '.raw'}
        matching_files = []
        
        for blob in blobs:
            blob_name = blob.name
            file_name = Path(blob_name).name
            file_extension = Path(blob_name).suffix.lower()
            
            # Check if file matches our criteria
            if (file_name.startswith(self.file_prefix_filter) and 
                file_extension in audio_extensions and
                not blob_name.endswith('/')):  # Exclude directories
                matching_files.append(blob_name)
        
        self.logger.info("Found matching audio files", count=len(matching_files))
        return matching_files
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def download_file(self, blob_name: str, local_path: Optional[str] = None) -> str:
        """Download a file from GCS to local storage.
        
        Args:
            blob_name: Name of the blob in GCS.
            local_path: Local path to save the file. If None, creates a temp file.
            
        Returns:
            Local file path where the file was downloaded.
        """
        self.logger.debug("Downloading file from GCS", blob_name=blob_name)
        
        blob = self.input_bucket.blob(blob_name)
        
        if local_path is None:
            # Create a temporary file
            suffix = Path(blob_name).suffix
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            local_path = temp_file.name
            temp_file.close()
        
        # Ensure directory exists
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Download the file
        await sync_to_async(blob.download_to_filename)(local_path)
        
        file_size = os.path.getsize(local_path)
        self.logger.info("File downloaded successfully",
                        blob_name=blob_name,
                        local_path=local_path,
                        size_bytes=file_size)
        
        return local_path
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def upload_file(self, local_path: str, blob_name: str, 
                         content_type: Optional[str] = None) -> str:
        """Upload a file from local storage to GCS.
        
        Args:
            local_path: Local file path to upload.
            blob_name: Name for the blob in GCS.
            content_type: Content type for the blob.
            
        Returns:
            GCS URI of the uploaded file.
        """
        self.logger.debug("Uploading file to GCS", 
                         local_path=local_path, 
                         blob_name=blob_name)
        
        # Add output folder prefix if specified
        if self.output_folder:
            blob_name = f"{self.output_folder.rstrip('/')}/{blob_name}"
        
        blob = self.output_bucket.blob(blob_name)
        
        if content_type:
            blob.content_type = content_type
        
        # Upload the file
        await sync_to_async(blob.upload_from_filename)(local_path)
        
        gcs_uri = f"gs://{self.output_bucket_name}/{blob_name}"
        
        self.logger.info("File uploaded successfully",
                        local_path=local_path,
                        gcs_uri=gcs_uri)
        
        return gcs_uri
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def upload_json_data(self, data: Dict[Any, Any], blob_name: str) -> str:
        """Upload JSON data directly to GCS.
        
        Args:
            data: Dictionary data to upload as JSON.
            blob_name: Name for the blob in GCS.
            
        Returns:
            GCS URI of the uploaded file.
        """
        import json
        
        self.logger.debug("Uploading JSON data to GCS", blob_name=blob_name)
        
        # Add output folder prefix if specified
        if self.output_folder:
            blob_name = f"{self.output_folder.rstrip('/')}/{blob_name}"
        
        blob = self.output_bucket.blob(blob_name)
        blob.content_type = 'application/json'
        
        # Convert data to JSON string
        json_string = json.dumps(data, indent=2, ensure_ascii=False)
        
        # Upload the data
        await sync_to_async(blob.upload_from_string)(json_string)
        
        gcs_uri = f"gs://{self.output_bucket_name}/{blob_name}"
        
        self.logger.info("JSON data uploaded successfully",
                        gcs_uri=gcs_uri,
                        data_size=len(json_string))
        
        return gcs_uri
    
    async def get_file_metadata(self, blob_name: str) -> Dict[str, Any]:
        """Get metadata for a file in GCS.
        
        Args:
            blob_name: Name of the blob in GCS.
            
        Returns:
            Dictionary containing file metadata.
        """
        blob = self.input_bucket.blob(blob_name)
        
        # Reload to get latest metadata
        await sync_to_async(blob.reload)()
        
        metadata = {
            'name': blob.name,
            'size': blob.size,
            'content_type': blob.content_type,
            'created': blob.time_created.isoformat() if blob.time_created else None,
            'updated': blob.updated.isoformat() if blob.updated else None,
            'etag': blob.etag,
            'generation': blob.generation,
            'md5_hash': blob.md5_hash,
            'crc32c': blob.crc32c
        }
        
        return metadata
    
    async def cleanup_temp_file(self, file_path: str) -> None:
        """Clean up a temporary file.
        
        Args:
            file_path: Path to the temporary file to delete.
        """
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                self.logger.debug("Temporary file cleaned up", file_path=file_path)
        except OSError as e:
            self.logger.warning("Failed to clean up temporary file", 
                              file_path=file_path, error=str(e))
    
    async def batch_download_files(self, blob_names: List[str]) -> List[str]:
        """Download multiple files concurrently.
        
        Args:
            blob_names: List of blob names to download.
            
        Returns:
            List of local file paths where files were downloaded.
        """
        from ..utils.async_helpers import AsyncTaskManager
        
        task_manager = AsyncTaskManager(max_concurrent_tasks=5)
        
        # Create download tasks
        download_tasks = [self.download_file(blob_name) for blob_name in blob_names]
        
        # Execute downloads concurrently
        local_paths = await task_manager.run_tasks(download_tasks)
        
        return local_paths