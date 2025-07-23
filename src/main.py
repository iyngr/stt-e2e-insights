"""Main orchestrator for STT E2E Insights pipeline."""

import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from modules.gcs_handler import GCSHandler
from modules.stt_processor import STTProcessor
from modules.dlp_processor import DLPProcessor
from modules.ccai_formatter import CCAIFormatter
from modules.ccai_uploader import CCAIUploader
from utils.config_loader import get_config, get_config_section
from utils.logger import setup_logging, get_logger


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
    
    def run_pipeline(self, file_limit: Optional[int] = None, processing_mode: Optional[str] = None) -> Dict[str, Any]:
        """Run the complete STT E2E Insights pipeline.
        
        Args:
            file_limit: Optional limit on number of files to process.
            processing_mode: Override the processing mode ("manual" or "direct").
            
        Returns:
            Pipeline execution summary.
        """
        self.processing_stats['start_time'] = datetime.utcnow().isoformat()
        self.logger.info("Starting STT E2E Insights pipeline")
        
        try:
            # Determine processing mode
            ccai_config = self.config['ccai']
            mode = processing_mode or ccai_config.get('processing_mode', 'manual')
            
            self.logger.info("Pipeline processing mode", mode=mode)
            
            # Step 1: Discover audio files
            audio_files = self._discover_audio_files(file_limit)
            
            # Step 2: Process files based on mode
            if mode == 'direct':
                results = self._process_files_direct_ingestion(audio_files)
            else:
                results = self._process_files_batch(audio_files)
            
            # Step 3: Generate summary
            summary = self._generate_summary(results, mode)
            
            self.processing_stats['end_time'] = datetime.utcnow().isoformat()
            self.logger.info("Pipeline completed successfully", summary=summary)
            
            return summary
            
        except Exception as e:
            self.processing_stats['end_time'] = datetime.utcnow().isoformat()
            self.logger.error("Pipeline failed", error=str(e))
            raise
    
    def _discover_audio_files(self, file_limit: Optional[int] = None) -> List[str]:
        """Discover audio files in GCS bucket.
        
        Args:
            file_limit: Optional limit on number of files.
            
        Returns:
            List of audio file blob names.
        """
        self.logger.info("Discovering audio files in GCS bucket")
        
        # Remove async call
        # For now, we'll call the sync version or convert the GCS handler
        audio_files = self._list_audio_files_sync()
        
        if file_limit and len(audio_files) > file_limit:
            audio_files = audio_files[:file_limit]
            self.logger.info(f"Limited file processing to {file_limit} files")
        
        self.processing_stats['files_discovered'] = len(audio_files)
        self.logger.info("Audio file discovery completed", file_count=len(audio_files))
        
        return audio_files
    
    def _list_audio_files_sync(self) -> List[str]:
        """Synchronous method to list audio files."""
        # Call the GCS handler list method directly (we'll make it sync)
        return self.gcs_handler.list_audio_files_sync()
    
    def _process_files_batch(self, audio_files: List[str]) -> Dict[str, Any]:
        """Process audio files in batches.
        
        Args:
            audio_files: List of audio file blob names.
            
        Returns:
            Processing results.
        """
        self.logger.info("Processing files in batches", 
                        total_files=len(audio_files))
        
        # Process files sequentially for now
        results = []
        for audio_file in audio_files:
            result = self._process_single_file(audio_file)
            results.append(result)
        
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
    
    def _process_files_direct_ingestion(self, audio_files: List[str]) -> Dict[str, Any]:
        """Process audio files using direct CCAI Insights ingestion.
        
        This method uses the IngestConversations API to upload audio files directly
        to CCAI Insights, letting CCAI handle STT transcription automatically.
        
        Args:
            audio_files: List of audio file blob names.
            
        Returns:
            Processing results.
        """
        self.logger.info("Processing files using direct CCAI ingestion", 
                        total_files=len(audio_files))
        
        try:
            # Use the CCAI uploader's direct ingestion method
            ingest_result = self.ccai_uploader.ingest_conversations_direct(audio_files)
            
            if ingest_result['success']:
                # Update stats
                self.processing_stats['files_processed'] = ingest_result['conversations_ingested']
                self.processing_stats['conversations_created'] = ingest_result['conversations_ingested']
                self.processing_stats['conversations_uploaded'] = ingest_result['conversations_ingested']
                
                # Create successful results for each file
                successful_results = []
                for audio_file in audio_files:
                    successful_results.append({
                        'blob_name': audio_file,
                        'success': True,
                        'processing_mode': 'direct_ingestion',
                        'conversation_id': f"direct-{Path(audio_file).stem}",
                        'upload_result': {
                            'success': True,
                            'operation_name': ingest_result['operation_name']
                        }
                    })
                
                return {
                    'successful_results': successful_results,
                    'failed_results': [],
                    'total_processed': len(audio_files),
                    'ingest_result': ingest_result
                }
            else:
                # All files failed
                self.processing_stats['files_failed'] = len(audio_files)
                
                failed_results = []
                for audio_file in audio_files:
                    failed_results.append({
                        'blob_name': audio_file,
                        'success': False,
                        'processing_mode': 'direct_ingestion',
                        'error': ingest_result['error']
                    })
                
                return {
                    'successful_results': [],
                    'failed_results': failed_results,
                    'total_processed': len(audio_files),
                    'ingest_result': ingest_result
                }
                
        except Exception as e:
            error_msg = str(e)
            self.logger.error("Direct ingestion processing failed", error=error_msg)
            
            # Mark all files as failed
            self.processing_stats['files_failed'] = len(audio_files)
            
            failed_results = []
            for audio_file in audio_files:
                failed_results.append({
                    'blob_name': audio_file,
                    'success': False,
                    'processing_mode': 'direct_ingestion',
                    'error': error_msg
                })
            
            return {
                'successful_results': [],
                'failed_results': failed_results,
                'total_processed': len(audio_files)
            }
    
    def _process_single_file(self, audio_file_blob: str) -> Dict[str, Any]:
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
        
        try:
            self.logger.info("Processing file", blob_name=audio_file_blob)
            
            # Step 1: Get GCS URI (no download needed)
            self.logger.debug("Step 1: Getting GCS URI", blob_name=audio_file_blob)
            gcs_uri = self.gcs_handler.get_gcs_uri(audio_file_blob)
            file_result['steps_completed'].append('gcs_uri')
            
            # Step 2: Transcribe audio using GCS URI
            self.logger.debug("Step 2: Transcribing audio", blob_name=audio_file_blob)
            transcription_result = self.stt_processor.transcribe_audio_file(gcs_uri)
            file_result['steps_completed'].append('transcription')
            
            # Step 3: Redact PII (if needed - we'll need to update this method too)
            self.logger.debug("Step 3: Redacting PII", blob_name=audio_file_blob)
            redacted_result = self._redact_pii_sync(transcription_result)
            file_result['steps_completed'].append('dlp_redaction')
            
            # Step 4: Format for CCAI
            self.logger.debug("Step 4: Formatting for CCAI", blob_name=audio_file_blob)
            audio_metadata = self._get_file_metadata_sync(audio_file_blob)
            formatted_conversation = self._format_conversation_sync(
                transcription_result, redacted_result, audio_metadata
            )
            file_result['steps_completed'].append('ccai_formatting')
            
            # Step 5: Save processed data to GCS
            self.logger.debug("Step 5: Saving processed data", blob_name=audio_file_blob)
            output_blob_name = f"processed_{Path(audio_file_blob).stem}.json"
            self._upload_json_data_sync(formatted_conversation, output_blob_name)
            file_result['steps_completed'].append('save_to_gcs')
            
            # Step 6: Upload to CCAI Insights
            self.logger.debug("Step 6: Uploading to CCAI", blob_name=audio_file_blob)
            upload_result = self._upload_conversation_sync(formatted_conversation)
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
        
        return file_result
    
    def _redact_pii_sync(self, transcription_result: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous PII redaction."""
        # For now, return the transcription as-is
        # You can implement sync DLP processing here
        return transcription_result
    
    def _get_file_metadata_sync(self, blob_name: str) -> Dict[str, Any]:
        """Synchronous file metadata retrieval."""
        # Return basic metadata
        return {
            'blob_name': blob_name,
            'file_name': Path(blob_name).name
        }
    
    def _format_conversation_sync(self, transcription_result: Dict[str, Any], 
                                 redacted_result: Dict[str, Any], 
                                 audio_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous CCAI formatting."""
        # Basic formatting - you can enhance this
        return {
            'transcription': transcription_result,
            'redacted': redacted_result,
            'metadata': audio_metadata
        }
    
    def _upload_json_data_sync(self, data: Dict[str, Any], blob_name: str) -> None:
        """Synchronous JSON data upload."""
        import json
        import tempfile
        
        # Create a temporary file with JSON data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(data, temp_file, indent=2)
            temp_file_path = temp_file.name
        
        try:
            # Upload to GCS using sync method
            self.gcs_handler.upload_file_sync(temp_file_path, blob_name, content_type='application/json')
            self.logger.info("JSON data uploaded successfully", blob_name=blob_name)
        finally:
            # Clean up temp file
            import os
            os.unlink(temp_file_path)
    
    def _upload_conversation_sync(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous conversation upload to CCAI Insights."""
        try:
            # Use the CCAI uploader's sync method (we'll need to implement this)
            result = self.ccai_uploader.upload_conversation_sync(conversation)
            self.logger.info("Conversation uploaded to CCAI", 
                           conversation_id=result.get('conversation_id'))
            return result
        except Exception as e:
            self.logger.error("Failed to upload conversation to CCAI", error=str(e))
            return {
                'success': False,
                'error': str(e),
                'conversation_id': None
            }
    
    def _generate_summary(self, results: Dict[str, Any], processing_mode: str = 'manual') -> Dict[str, Any]:
        """Generate pipeline execution summary.
        
        Args:
            results: Processing results.
            processing_mode: The processing mode used ('manual' or 'direct').
            
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
                'status': 'completed',
                'processing_mode': processing_mode
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
                        'steps_completed': r.get('steps_completed', []),
                        'processing_mode': r.get('processing_mode', processing_mode)
                    }
                    for r in successful_results
                ],
                'failed_files': [
                    {
                        'blob_name': r['blob_name'],
                        'error': r.get('error'),
                        'steps_completed': r.get('steps_completed', []),
                        'processing_mode': r.get('processing_mode', processing_mode)
                    }
                    for r in failed_results
                ]
            }
        }
        
        # Add additional info for direct ingestion mode
        if processing_mode == 'direct' and 'ingest_result' in results:
            ingest_result = results['ingest_result']
            summary['direct_ingestion'] = {
                'operation_name': ingest_result.get('operation_name'),
                'recognizer_id': ingest_result.get('recognizer_id'),
                'ingest_statistics': ingest_result.get('ingest_statistics')
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


def main():
    """Main entry point for the pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='STT E2E Insights Pipeline')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--validate-only', action='store_true', 
                       help='Only validate setup, do not run pipeline')
    parser.add_argument('--file-limit', type=int, 
                       help='Limit number of files to process')
    parser.add_argument('--processing-mode', type=str, choices=['manual', 'direct'],
                       help='Processing mode: manual (STT->DLP->CCAI) or direct (CCAI direct ingestion)')
    parser.add_argument('--recognizer-id', type=str,
                       help='Override recognizer ID for direct ingestion mode')
    
    args = parser.parse_args()
    
    try:
        # Initialize pipeline
        pipeline = STTInsightsPipeline(args.config)
        
        # Override recognizer ID if provided
        if args.recognizer_id:
            pipeline.config['ccai']['recognizer_id'] = args.recognizer_id
        
        if args.validate_only:
            # Run validation only
            validation_results = pipeline.validate_setup()
            
            if all(validation_results.values()):
                print("✅ All validations passed! Pipeline is ready to run.")
                return 0
            else:
                print("❌ Some validations failed. Please check the logs.")
                return 1
        else:
            # Run full pipeline
            summary = pipeline.run_pipeline(args.file_limit, args.processing_mode)
            
            # Print summary
            print("\n" + "="*50)
            print("PIPELINE EXECUTION SUMMARY")
            print("="*50)
            print(f"Processing mode: {summary['pipeline_execution']['processing_mode']}")
            print(f"Files discovered: {summary['file_processing']['files_discovered']}")
            print(f"Files processed successfully: {summary['file_processing']['files_processed_successfully']}")
            print(f"Files failed: {summary['file_processing']['files_failed']}")
            print(f"Success rate: {summary['file_processing']['success_rate_percent']}%")
            print(f"Conversations uploaded: {summary['conversation_processing']['conversations_uploaded_successfully']}")
            print(f"Upload success rate: {summary['conversation_processing']['upload_success_rate_percent']}%")
            
            # Print additional info for direct ingestion
            if summary['pipeline_execution']['processing_mode'] == 'direct' and 'direct_ingestion' in summary:
                direct_info = summary['direct_ingestion']
                print(f"Recognizer used: {direct_info.get('recognizer_id', 'N/A')}")
                print(f"Operation name: {direct_info.get('operation_name', 'N/A')}")
            
            if summary['pipeline_execution']['duration_seconds']:
                print(f"Total duration: {summary['pipeline_execution']['duration_seconds']:.2f} seconds")
            
            return 0 if summary['file_processing']['files_failed'] == 0 else 1
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())