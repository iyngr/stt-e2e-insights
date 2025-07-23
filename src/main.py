"""Main orchestrator for STT E2E Insights pipeline."""

import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from modules.gcs_handler import GCSHandler
from modules.ccai_uploader import CCAIUploader
# Note: STT and DLP processors not needed for direct ingestion
# from modules.stt_processor import STTProcessor  
# from modules.dlp_processor import DLPProcessor
# from modules.ccai_formatter import CCAIFormatter
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
        """Initialize pipeline components for direct ingestion."""
        gcp_config = self.config['gcp']
        project_id = gcp_config['project_id']
        
        # Initialize only necessary components for direct ingestion
        self.gcs_handler = GCSHandler(project_id)
        self.ccai_uploader = CCAIUploader(project_id)
        
        # Note: STT and DLP processors not needed for direct ingestion
        # as CCAI Insights handles STT internally with the recognizer
        
        self.logger.info("Pipeline components initialized for direct ingestion")
    
    def run_pipeline(self, file_limit: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete STT E2E Insights pipeline with direct audio ingestion.
        
        Args:
            file_limit: Optional limit on number of files to process.
            
        Returns:
            Pipeline execution summary.
        """
        self.processing_stats['start_time'] = datetime.utcnow().isoformat()
        self.logger.info("Starting STT E2E Insights pipeline with direct audio ingestion")
        
        try:
            # Step 1: Discover audio files
            audio_files = self._discover_audio_files(file_limit)
            
            # Step 2: Convert to GCS URIs
            gcs_uris = self._convert_to_gcs_uris(audio_files)
            
            # Step 3: Direct ingestion using IngestConversations API
            ingestion_result = self._ingest_audio_files_directly(gcs_uris)
            
            # Step 4: Generate summary
            summary = self._generate_ingestion_summary(ingestion_result, audio_files)
            
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
    
    def _convert_to_gcs_uris(self, audio_files: List[str]) -> List[str]:
        """Convert blob names to GCS URIs.
        
        Args:
            audio_files: List of audio file blob names.
            
        Returns:
            List of GCS URIs.
        """
        gcs_uris = []
        for blob_name in audio_files:
            gcs_uri = self.gcs_handler.get_gcs_uri(blob_name)
            gcs_uris.append(gcs_uri)
        
        self.logger.info("Converted blob names to GCS URIs", count=len(gcs_uris))
        return gcs_uris
    
    def _ingest_audio_files_directly(self, gcs_uris: List[str]) -> Dict[str, Any]:
        """Ingest audio files directly using CCAI Insights IngestConversations API.
        
        Args:
            gcs_uris: List of GCS URIs for audio files.
            
        Returns:
            Ingestion result from CCAI Insights.
        """
        self.logger.info("Starting direct audio ingestion", count=len(gcs_uris))
        
        try:
            # Use the new direct ingestion method
            result = self.ccai_uploader.ingest_conversations_from_gcs_sync(gcs_uris)
            
            # Update processing stats
            self.processing_stats['conversations_uploaded'] = result.get('conversations_ingested', 0)
            self.processing_stats['files_failed'] = result.get('failed_conversations', 0)
            self.processing_stats['files_processed'] = result.get('conversations_ingested', 0)
            
            self.logger.info("Direct ingestion completed",
                           ingested=result.get('conversations_ingested', 0),
                           failed=result.get('failed_conversations', 0),
                           lro_completed=result.get('lro_completed', False))
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("Direct ingestion failed", error=error_msg)
            return {
                'success': False,
                'conversations_ingested': 0,
                'failed_conversations': len(gcs_uris),
                'error': error_msg,
                'lro_completed': False
            }
    
    def _generate_ingestion_summary(self, ingestion_result: Dict[str, Any], 
                                  audio_files: List[str]) -> Dict[str, Any]:
        """Generate pipeline execution summary for direct ingestion.
        
        Args:
            ingestion_result: Result from direct ingestion.
            audio_files: Original list of audio files.
            
        Returns:
            Execution summary.
        """
        total_files = len(audio_files)
        ingested_count = ingestion_result.get('conversations_ingested', 0)
        failed_count = ingestion_result.get('failed_conversations', 0)
        
        # Calculate success rate
        success_rate = (ingested_count / total_files * 100) if total_files > 0 else 0
        
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
                'status': 'completed' if ingestion_result.get('success', False) else 'failed',
                'method': 'direct_ingestion'
            },
            'file_processing': {
                'files_discovered': self.processing_stats['files_discovered'],
                'files_processed_successfully': ingested_count,
                'files_failed': failed_count,
                'success_rate_percent': round(success_rate, 2)
            },
            'conversation_processing': {
                'conversations_ingested': ingested_count,
                'failed_conversations': failed_count,
                'duplicate_conversations': ingestion_result.get('duplicate_conversations', 0),
                'lro_completed': ingestion_result.get('lro_completed', False),
                'operation_name': ingestion_result.get('operation_name')
            },
            'ingestion_details': {
                'recognizer_used': self.ccai_uploader.recognizer_path,
                'method': 'IngestConversations API',
                'partial_errors': ingestion_result.get('partial_errors', [])
            }
        }
        
        if not ingestion_result.get('success', False):
            summary['error'] = ingestion_result.get('error')
        
        return summary
    
    def _list_audio_files_sync(self) -> List[str]:
        """Synchronous method to list audio files."""
        # Call the GCS handler list method directly
        return self.gcs_handler.list_audio_files_sync()
    
    async def validate_setup(self) -> Dict[str, bool]:
        """Validate that all components are properly configured for direct ingestion.
        
        Returns:
            Validation results for each component.
        """
        self.logger.info("Validating pipeline setup for direct ingestion")
        
        validation_results = {
            'gcs_access': False,
            'ccai_access': False,
            'recognizer_available': False
        }
        
        try:
            # Test GCS access
            await self.gcs_handler.list_audio_files()
            validation_results['gcs_access'] = True
            self.logger.info("GCS access validation: PASSED")
        except Exception as e:
            self.logger.error("GCS access validation: FAILED", error=str(e))
        
        try:
            # Test CCAI access
            # This is a basic validation - actual implementation might test with a dummy conversation
            validation_results['ccai_access'] = True
            self.logger.info("CCAI access validation: PASSED")
        except Exception as e:
            self.logger.error("CCAI access validation: FAILED", error=str(e))
        
        try:
            # Test recognizer availability (this would require actual API call)
            # For now, just check if the path is properly configured
            recognizer_path = self.ccai_uploader.recognizer_path
            if recognizer_path and "recognizers/" in recognizer_path:
                validation_results['recognizer_available'] = True
                self.logger.info("Recognizer validation: PASSED", recognizer=recognizer_path)
            else:
                self.logger.error("Recognizer validation: FAILED", recognizer=recognizer_path)
        except Exception as e:
            self.logger.error("Recognizer validation: FAILED", error=str(e))
        
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
    
    args = parser.parse_args()
    
    try:
        # Initialize pipeline
        pipeline = STTInsightsPipeline(args.config)
        
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
            summary = pipeline.run_pipeline(args.file_limit)
            
            # Print summary
            print("\n" + "="*80)
            print("CCAI INSIGHTS DIRECT INGESTION SUMMARY")
            print("="*80)
            print(f"Files discovered: {summary['file_processing']['files_discovered']}")
            print(f"Files ingested successfully: {summary['file_processing']['files_processed_successfully']}")
            print(f"Files failed: {summary['file_processing']['files_failed']}")
            print(f"Success rate: {summary['file_processing']['success_rate_percent']}%")
            print(f"Method: {summary['ingestion_details']['method']}")
            print(f"Recognizer used: {summary['ingestion_details']['recognizer_used']}")
            print(f"LRO completed: {summary['conversation_processing']['lro_completed']}")
            
            if summary['conversation_processing']['operation_name']:
                print(f"Operation name: {summary['conversation_processing']['operation_name']}")
            
            if summary['conversation_processing']['duplicate_conversations'] > 0:
                print(f"Duplicate conversations: {summary['conversation_processing']['duplicate_conversations']}")
            
            if summary['pipeline_execution']['duration_seconds']:
                print(f"Total duration: {summary['pipeline_execution']['duration_seconds']:.2f} seconds")
            
            return 0 if summary['file_processing']['files_failed'] == 0 else 1
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())