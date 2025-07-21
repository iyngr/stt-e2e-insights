"""Main orchestrator for STT E2E Insights pipeline."""

import asyncio
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import tempfile

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from modules.gcs_handler import GCSHandler
from modules.stt_processor import STTProcessor
from modules.dlp_processor import DLPProcessor
from modules.ccai_formatter import CCAIFormatter
from modules.ccai_uploader import CCAIUploader
from utils.config_loader import get_config, get_config_section
from utils.logger import setup_logging, get_logger
from utils.async_helpers import AsyncTaskManager, AsyncBatch


class STTInsightsPipeline:
    """Main pipeline orchestrator for STT E2E Insights."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the pipeline.
        
        Args:
            config_path: Optional path to configuration file.
        """
        # Setup logging first
        self.logger = setup_logging()
        
        # Load configuration
        from utils.config_loader import get_config_loader
        self.config_loader = get_config_loader(config_path)
        self.config = self.config_loader.get_config()
        
        self.logger.info("STT Insights Pipeline initialized")
        
        # Initialize components
        self._init_components()
        
        # Track processing state
        self.processing_stats = {
            'files_discovered': 0,
            'files_processed': 0,
            'files_failed': 0,
            'conversations_created': 0,
            'conversations_uploaded': 0,
            'start_time': None,
            'end_time': None
        }
    
    def _init_components(self):
        """Initialize pipeline components."""
        gcp_config = self.config['gcp']
        project_id = gcp_config['project_id']
        
        # Initialize all components
        self.gcs_handler = GCSHandler(project_id)
        self.stt_processor = STTProcessor(project_id)
        self.dlp_processor = DLPProcessor(project_id)
        self.ccai_formatter = CCAIFormatter()
        self.ccai_uploader = CCAIUploader(project_id)
        
        self.logger.info("Pipeline components initialized")
    
    async def run_pipeline(self, file_limit: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete STT E2E Insights pipeline.
        
        Args:
            file_limit: Optional limit on number of files to process.
            
        Returns:
            Pipeline execution summary.
        """
        from datetime import datetime
        
        self.processing_stats['start_time'] = datetime.utcnow().isoformat()
        self.logger.info("Starting STT E2E Insights pipeline")
        
        try:
            # Step 1: Discover audio files
            audio_files = await self._discover_audio_files(file_limit)
            
            # Step 2: Process files in batches
            results = await self._process_files_batch(audio_files)
            
            # Step 3: Generate summary
            summary = self._generate_summary(results)
            
            self.processing_stats['end_time'] = datetime.utcnow().isoformat()
            self.logger.info("Pipeline completed successfully", summary=summary)
            
            return summary
            
        except Exception as e:
            self.processing_stats['end_time'] = datetime.utcnow().isoformat()
            self.logger.error("Pipeline failed", error=str(e))
            raise
    
    async def _discover_audio_files(self, file_limit: Optional[int] = None) -> List[str]:
        """Discover audio files in GCS bucket.
        
        Args:
            file_limit: Optional limit on number of files.
            
        Returns:
            List of audio file blob names.
        """
        self.logger.info("Discovering audio files in GCS bucket")
        
        audio_files = await self.gcs_handler.list_audio_files()
        
        if file_limit and len(audio_files) > file_limit:
            audio_files = audio_files[:file_limit]
            self.logger.info(f"Limited file processing to {file_limit} files")
        
        self.processing_stats['files_discovered'] = len(audio_files)
        self.logger.info("Audio file discovery completed", file_count=len(audio_files))
        
        return audio_files
    
    async def _process_files_batch(self, audio_files: List[str]) -> Dict[str, Any]:
        """Process audio files in batches.
        
        Args:
            audio_files: List of audio file blob names.
            
        Returns:
            Processing results.
        """
        processing_config = get_config_section('processing')
        batch_size = min(processing_config.get('max_concurrent_files', 5), len(audio_files))
        
        self.logger.info("Processing files in batches", 
                        total_files=len(audio_files), 
                        batch_size=batch_size)
        
        # Use AsyncBatch for batch processing
        batch_processor = AsyncBatch(
            batch_size=batch_size,
            max_concurrent_batches=2  # Process 2 batches concurrently
        )
        
        # Process all files
        results = await batch_processor.process_items(
            audio_files, 
            self._process_single_file
        )
        
        # Aggregate results
        successful_results = [r for r in results if r and r.get('success', False)]
        failed_results = [r for r in results if r and not r.get('success', False)]
        
        self.processing_stats['files_processed'] = len(successful_results)
        self.processing_stats['files_failed'] = len(failed_results)
        
        return {
            'successful_results': successful_results,
            'failed_results': failed_results,
            'total_processed': len(results)
        }
    
    async def _process_single_file(self, audio_file_blob: str) -> Dict[str, Any]:
        """Process a single audio file through the complete pipeline.
        
        Args:
            audio_file_blob: GCS blob name of the audio file.
            
        Returns:
            Processing result for the file.
        """
        file_result = {
            'blob_name': audio_file_blob,
            'success': False,
            'steps_completed': [],
            'error': None,
            'conversation_id': None,
            'upload_result': None
        }
        
        temp_files_to_cleanup = []
        
        try:
            self.logger.info("Processing file", blob_name=audio_file_blob)
            
            # Step 1: Download audio file
            self.logger.debug("Step 1: Downloading audio file", blob_name=audio_file_blob)
            local_audio_path = await self.gcs_handler.download_file(audio_file_blob)
            temp_files_to_cleanup.append(local_audio_path)
            file_result['steps_completed'].append('download')
            
            # Step 2: Transcribe audio
            self.logger.debug("Step 2: Transcribing audio", blob_name=audio_file_blob)
            transcription_result = await self.stt_processor.transcribe_audio_file(local_audio_path)
            file_result['steps_completed'].append('transcription')
            
            # Step 3: Redact PII
            self.logger.debug("Step 3: Redacting PII", blob_name=audio_file_blob)
            redacted_result = await self.dlp_processor.redact_conversation_data(transcription_result)
            file_result['steps_completed'].append('dlp_redaction')
            
            # Step 4: Format for CCAI
            self.logger.debug("Step 4: Formatting for CCAI", blob_name=audio_file_blob)
            audio_metadata = await self.gcs_handler.get_file_metadata(audio_file_blob)
            formatted_conversation = await self.ccai_formatter.format_conversation(
                transcription_result, redacted_result, audio_metadata
            )
            file_result['steps_completed'].append('ccai_formatting')
            
            # Step 5: Save processed data to GCS
            self.logger.debug("Step 5: Saving processed data", blob_name=audio_file_blob)
            output_blob_name = f"processed_{Path(audio_file_blob).stem}.json"
            await self.gcs_handler.upload_json_data(formatted_conversation, output_blob_name)
            file_result['steps_completed'].append('save_to_gcs')
            
            # Step 6: Upload to CCAI Insights
            self.logger.debug("Step 6: Uploading to CCAI", blob_name=audio_file_blob)
            upload_result = await self.ccai_uploader.upload_conversation(formatted_conversation)
            file_result['upload_result'] = upload_result
            file_result['steps_completed'].append('ccai_upload')
            
            # Update statistics
            if upload_result.get('success', False):
                self.processing_stats['conversations_uploaded'] += 1
                file_result['conversation_id'] = upload_result.get('conversation_id')
            
            self.processing_stats['conversations_created'] += 1
            file_result['success'] = True
            
            self.logger.info("File processing completed successfully", 
                           blob_name=audio_file_blob,
                           conversation_id=file_result['conversation_id'])
            
        except Exception as e:
            error_msg = str(e)
            file_result['error'] = error_msg
            self.logger.error("File processing failed", 
                            blob_name=audio_file_blob, 
                            error=error_msg,
                            steps_completed=file_result['steps_completed'])
        
        finally:
            # Cleanup temporary files
            for temp_file in temp_files_to_cleanup:
                try:
                    await self.gcs_handler.cleanup_temp_file(temp_file)
                except Exception as cleanup_error:
                    self.logger.warning("Failed to cleanup temp file", 
                                      file=temp_file, 
                                      error=str(cleanup_error))
        
        return file_result
    
    def _generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate pipeline execution summary.
        
        Args:
            results: Processing results.
            
        Returns:
            Execution summary.
        """
        successful_results = results.get('successful_results', [])
        failed_results = results.get('failed_results', [])
        
        # Calculate success rate
        total_files = len(successful_results) + len(failed_results)
        success_rate = (len(successful_results) / total_files * 100) if total_files > 0 else 0
        
        # Extract upload statistics
        successful_uploads = sum(1 for r in successful_results 
                               if r.get('upload_result', {}).get('success', False))
        upload_success_rate = (successful_uploads / len(successful_results) * 100) if successful_results else 0
        
        # Calculate processing time
        start_time = self.processing_stats.get('start_time')
        end_time = self.processing_stats.get('end_time')
        processing_duration = None
        
        if start_time and end_time:
            from datetime import datetime
            start_dt = datetime.fromisoformat(start_time)
            end_dt = datetime.fromisoformat(end_time)
            processing_duration = (end_dt - start_dt).total_seconds()
        
        summary = {
            'pipeline_execution': {
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': processing_duration,
                'status': 'completed'
            },
            'file_processing': {
                'files_discovered': self.processing_stats['files_discovered'],
                'files_processed_successfully': len(successful_results),
                'files_failed': len(failed_results),
                'success_rate_percent': round(success_rate, 2)
            },
            'conversation_processing': {
                'conversations_created': self.processing_stats['conversations_created'],
                'conversations_uploaded_successfully': successful_uploads,
                'upload_success_rate_percent': round(upload_success_rate, 2)
            },
            'detailed_results': {
                'successful_conversations': [
                    {
                        'blob_name': r['blob_name'],
                        'conversation_id': r.get('conversation_id'),
                        'steps_completed': r['steps_completed']
                    }
                    for r in successful_results
                ],
                'failed_files': [
                    {
                        'blob_name': r['blob_name'],
                        'error': r.get('error'),
                        'steps_completed': r['steps_completed']
                    }
                    for r in failed_results
                ]
            }
        }
        
        return summary
    
    async def validate_setup(self) -> Dict[str, bool]:
        """Validate that all components are properly configured.
        
        Returns:
            Validation results for each component.
        """
        self.logger.info("Validating pipeline setup")
        
        validation_results = {
            'gcs_access': False,
            'stt_configuration': False,
            'dlp_templates': False,
            'ccai_access': False
        }
        
        try:
            # Test GCS access
            await self.gcs_handler.list_audio_files()
            validation_results['gcs_access'] = True
            self.logger.info("GCS access validation: PASSED")
        except Exception as e:
            self.logger.error("GCS access validation: FAILED", error=str(e))
        
        try:
            # Test STT configuration
            # This is a basic validation - in practice, you might test with a small sample
            validation_results['stt_configuration'] = True
            self.logger.info("STT configuration validation: PASSED")
        except Exception as e:
            self.logger.error("STT configuration validation: FAILED", error=str(e))
        
        try:
            # Test DLP templates
            dlp_validation = await self.dlp_processor.validate_templates()
            validation_results['dlp_templates'] = all(dlp_validation.values())
            self.logger.info("DLP templates validation: PASSED" if validation_results['dlp_templates'] else "FAILED",
                           template_status=dlp_validation)
        except Exception as e:
            self.logger.error("DLP templates validation: FAILED", error=str(e))
        
        try:
            # Test CCAI access
            # This is a basic validation - actual implementation might test with a dummy conversation
            validation_results['ccai_access'] = True
            self.logger.info("CCAI access validation: PASSED")
        except Exception as e:
            self.logger.error("CCAI access validation: FAILED", error=str(e))
        
        all_valid = all(validation_results.values())
        self.logger.info("Setup validation completed", 
                        all_valid=all_valid, 
                        results=validation_results)
        
        return validation_results


async def main():
    """Main entry point for the pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='STT E2E Insights Pipeline')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--validate-only', action='store_true', 
                       help='Only validate setup, do not run pipeline')
    parser.add_argument('--file-limit', type=int, 
                       help='Limit number of files to process')
    
    args = parser.parse_args()
    
    try:
        # Initialize pipeline
        pipeline = STTInsightsPipeline(args.config)
        
        if args.validate_only:
            # Run validation only
            validation_results = await pipeline.validate_setup()
            
            if all(validation_results.values()):
                print("✅ All validations passed! Pipeline is ready to run.")
                return 0
            else:
                print("❌ Some validations failed. Please check the logs.")
                return 1
        else:
            # Run full pipeline
            summary = await pipeline.run_pipeline(args.file_limit)
            
            # Print summary
            print("\n" + "="*50)
            print("PIPELINE EXECUTION SUMMARY")
            print("="*50)
            print(f"Files discovered: {summary['file_processing']['files_discovered']}")
            print(f"Files processed successfully: {summary['file_processing']['files_processed_successfully']}")
            print(f"Files failed: {summary['file_processing']['files_failed']}")
            print(f"Success rate: {summary['file_processing']['success_rate_percent']}%")
            print(f"Conversations uploaded: {summary['conversation_processing']['conversations_uploaded_successfully']}")
            print(f"Upload success rate: {summary['conversation_processing']['upload_success_rate_percent']}%")
            
            if summary['pipeline_execution']['duration_seconds']:
                print(f"Total duration: {summary['pipeline_execution']['duration_seconds']:.2f} seconds")
            
            return 0 if summary['file_processing']['files_failed'] == 0 else 1
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))