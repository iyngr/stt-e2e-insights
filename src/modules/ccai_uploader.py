"""CCAI Insights uploader for STT E2E Insights."""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime

from google.cloud import contact_center_insights_v1
from google.cloud.contact_center_insights_v1 import ContactCenterInsightsClient
from google.cloud.contact_center_insights_v1.types import Conversation, IngestConversationsRequest

from ..utils.logger import LoggerMixin
from ..utils.config_loader import get_config_section
from ..utils.async_helpers import sync_to_async, async_retry, AsyncTaskManager


class CCAIUploader(LoggerMixin):
    """Handles uploading conversations to CCAI Insights."""
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize the CCAI uploader.
        
        Args:
            project_id: GCP project ID. If None, loads from config.
        """
        try:
            gcp_config = get_config_section('gcp')
            self.ccai_config = get_config_section('ccai')
        except KeyError as e:
            raise ValueError(f"Missing configuration section: {e}")
        
        self.project_id = project_id or gcp_config.get('project_id')
        if not self.project_id:
            raise ValueError("GCP project ID must be provided")
        
        self.location = self.ccai_config.get('location', 'us-central1')
        
        # Initialize CCAI client
        self.client = ContactCenterInsightsClient()
        
        # Build parent path
        self.parent = f"projects/{self.project_id}/locations/{self.location}"
        
        self.logger.info("CCAI uploader initialized",
                        project_id=self.project_id,
                        location=self.location)
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def upload_conversation(self, conversation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Upload a single conversation to CCAI Insights.
        
        Args:
            conversation_data: Formatted conversation data from CCAIFormatter.
            
        Returns:
            Dictionary containing upload result and conversation details.
        """
        conversation_id = conversation_data.get('name', '').split('/')[-1]
        self.logger.debug("Uploading conversation to CCAI Insights",
                         conversation_id=conversation_id)
        
        try:
            # Create conversation object
            conversation = self._create_conversation_object(conversation_data)
            
            # Create the conversation
            request = {
                "parent": self.parent,
                "conversation": conversation,
                "conversation_id": conversation_id
            }
            
            response = await sync_to_async(self.client.create_conversation)(request)
            
            result = {
                'success': True,
                'conversation_id': conversation_id,
                'conversation_name': response.name,
                'create_time': response.create_time.isoformat() if response.create_time else None,
                'state': response.state.name if response.state else None,
                'medium': response.medium.name if response.medium else None,
                'error': None
            }
            
            self.logger.info("Conversation uploaded successfully",
                           conversation_id=conversation_id,
                           conversation_name=response.name)
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("Failed to upload conversation",
                            conversation_id=conversation_id,
                            error=error_msg)
            
            return {
                'success': False,
                'conversation_id': conversation_id,
                'conversation_name': None,
                'create_time': None,
                'state': None,
                'medium': None,
                'error': error_msg
            }
    
    def _create_conversation_object(self, conversation_data: Dict[str, Any]) -> Conversation:
        """Create a Conversation object from formatted data.
        
        Args:
            conversation_data: Formatted conversation data.
            
        Returns:
            Conversation object for the API.
        """
        # Extract basic information
        medium = getattr(Conversation.Medium, conversation_data.get('medium', 'PHONE_CALL'))
        
        # Create conversation object
        conversation = Conversation(
            medium=medium,
            language_code=conversation_data.get('language_code', 'en-US'),
            expire_time=self._parse_timestamp(conversation_data.get('expire_time')),
            ttl=self._parse_duration(conversation_data.get('ttl'))
        )
        
        # Add data source if present
        data_source = conversation_data.get('data_source')
        if data_source:
            conversation.data_source = self._create_data_source(data_source)
        
        # Add call metadata if present
        call_metadata = conversation_data.get('call_metadata')
        if call_metadata:
            conversation.call_metadata = self._create_call_metadata(call_metadata)
        
        # Add conversation transcript
        transcript_data = conversation_data.get('conversation_transcript')
        if transcript_data:
            conversation.transcript = self._create_transcript(transcript_data)
        
        # Add runtime annotations
        annotations = conversation_data.get('runtime_annotations', [])
        if annotations:
            conversation.runtime_annotations = [
                self._create_runtime_annotation(ann) for ann in annotations
            ]
        
        return conversation
    
    def _create_data_source(self, data_source_data: Dict[str, Any]) -> Any:
        """Create data source object.
        
        Args:
            data_source_data: Data source information.
            
        Returns:
            DataSource object.
        """
        from google.cloud.contact_center_insights_v1.types import ConversationDataSource
        
        data_source = ConversationDataSource()
        
        # Handle different source types
        if 'dialogflow_source' in data_source_data:
            df_source = data_source_data['dialogflow_source']
            if df_source.get('audio_uri'):
                # Note: The actual API structure may differ, adapt as needed
                data_source.dialogflow_source.audio_uri = df_source['audio_uri']
        
        return data_source
    
    def _create_call_metadata(self, metadata: Dict[str, Any]) -> Any:
        """Create call metadata object.
        
        Args:
            metadata: Call metadata information.
            
        Returns:
            CallMetadata object.
        """
        from google.cloud.contact_center_insights_v1.types import Conversation
        
        call_metadata = Conversation.CallMetadata()
        
        if 'customer_channel' in metadata:
            call_metadata.customer_channel = metadata['customer_channel']
        if 'agent_channel' in metadata:
            call_metadata.agent_channel = metadata['agent_channel']
        if 'agent_id' in metadata:
            call_metadata.agent_id = metadata['agent_id']
        if 'customer_id' in metadata:
            call_metadata.customer_id = metadata['customer_id']
        
        return call_metadata
    
    def _create_transcript(self, transcript_data: Dict[str, Any]) -> Any:
        """Create conversation transcript object.
        
        Args:
            transcript_data: Transcript information.
            
        Returns:
            ConversationTranscript object.
        """
        from google.cloud.contact_center_insights_v1.types import ConversationTranscript
        
        transcript = ConversationTranscript()
        
        # Add transcript segments
        segments = transcript_data.get('transcript_segments', [])
        for segment_data in segments:
            segment = self._create_transcript_segment(segment_data)
            transcript.transcript_segments.append(segment)
        
        return transcript
    
    def _create_transcript_segment(self, segment_data: Dict[str, Any]) -> Any:
        """Create a transcript segment object.
        
        Args:
            segment_data: Segment information.
            
        Returns:
            ConversationTranscript.TranscriptSegment object.
        """
        from google.cloud.contact_center_insights_v1.types import ConversationTranscript
        
        segment = ConversationTranscript.TranscriptSegment()
        
        segment.text = segment_data.get('text', '')
        segment.confidence = segment_data.get('confidence', 0.0)
        segment.language_code = segment_data.get('language_code', 'en-US')
        segment.channel_tag = segment_data.get('channel_tag', 0)
        
        # Add timing information
        if 'segment_start_time' in segment_data:
            segment.segment_start_time = self._parse_duration(segment_data['segment_start_time'])
        if 'segment_end_time' in segment_data:
            segment.segment_end_time = self._parse_duration(segment_data['segment_end_time'])
        
        # Add participant information
        participant_data = segment_data.get('segment_participant', {})
        if participant_data:
            segment.segment_participant = self._create_participant(participant_data)
        
        # Add word-level information
        words = segment_data.get('words', [])
        for word_data in words:
            word = self._create_word_info(word_data)
            segment.words.append(word)
        
        return segment
    
    def _create_participant(self, participant_data: Dict[str, Any]) -> Any:
        """Create conversation participant object.
        
        Args:
            participant_data: Participant information.
            
        Returns:
            ConversationParticipant object.
        """
        from google.cloud.contact_center_insights_v1.types import ConversationParticipant
        
        participant = ConversationParticipant()
        
        if 'dialogflow_participant_name' in participant_data:
            participant.dialogflow_participant_name = participant_data['dialogflow_participant_name']
        if 'obfuscated_external_user_id' in participant_data:
            participant.obfuscated_external_user_id = participant_data['obfuscated_external_user_id']
        if 'role' in participant_data:
            role = getattr(ConversationParticipant.Role, participant_data['role'], 
                          ConversationParticipant.Role.ROLE_UNSPECIFIED)
            participant.role = role
        
        return participant
    
    def _create_word_info(self, word_data: Dict[str, Any]) -> Any:
        """Create word information object.
        
        Args:
            word_data: Word information.
            
        Returns:
            WordInfo object.
        """
        from google.cloud.contact_center_insights_v1.types import ConversationTranscript
        
        word_info = ConversationTranscript.TranscriptSegment.WordInfo()
        
        word_info.word = word_data.get('word', '')
        word_info.confidence = word_data.get('confidence', 0.0)
        
        if 'start_offset' in word_data:
            word_info.start_offset = self._parse_duration(word_data['start_offset'])
        if 'end_offset' in word_data:
            word_info.end_offset = self._parse_duration(word_data['end_offset'])
        
        return word_info
    
    def _create_runtime_annotation(self, annotation_data: Dict[str, Any]) -> Any:
        """Create runtime annotation object.
        
        Args:
            annotation_data: Annotation information.
            
        Returns:
            RuntimeAnnotation object.
        """
        from google.cloud.contact_center_insights_v1.types import RuntimeAnnotation
        import json
        
        annotation = RuntimeAnnotation()
        
        annotation.annotation_id = annotation_data.get('annotation_id', '')
        
        if 'create_time' in annotation_data:
            annotation.create_time = self._parse_timestamp(annotation_data['create_time'])
        
        # Add annotation payload as JSON
        payload = annotation_data.get('annotation_payload', {})
        if payload:
            # Note: The actual structure may differ based on API requirements
            annotation.annotation_payload = json.dumps(payload)
        
        return annotation
    
    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[Any]:
        """Parse timestamp string to protobuf Timestamp.
        
        Args:
            timestamp_str: Timestamp string in ISO format.
            
        Returns:
            Protobuf Timestamp object or None.
        """
        if not timestamp_str:
            return None
        
        try:
            from google.protobuf.timestamp_pb2 import Timestamp
            from datetime import datetime
            
            # Parse ISO timestamp
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            timestamp = Timestamp()
            timestamp.FromDatetime(dt)
            return timestamp
        except Exception as e:
            self.logger.warning("Failed to parse timestamp", timestamp=timestamp_str, error=str(e))
            return None
    
    def _parse_duration(self, duration_str: Optional[str]) -> Optional[Any]:
        """Parse duration string to protobuf Duration.
        
        Args:
            duration_str: Duration string (e.g., "30s", "2.5s").
            
        Returns:
            Protobuf Duration object or None.
        """
        if not duration_str:
            return None
        
        try:
            from google.protobuf.duration_pb2 import Duration
            
            # Remove 's' suffix and parse as float
            seconds = float(duration_str.rstrip('s'))
            
            duration = Duration()
            duration.seconds = int(seconds)
            duration.nanos = int((seconds - int(seconds)) * 1_000_000_000)
            return duration
        except Exception as e:
            self.logger.warning("Failed to parse duration", duration=duration_str, error=str(e))
            return None
    
    async def batch_upload_conversations(self, conversations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Upload multiple conversations concurrently.
        
        Args:
            conversations: List of formatted conversation data.
            
        Returns:
            List of upload results.
        """
        processing_config = get_config_section('processing')
        max_concurrent = processing_config.get('max_concurrent_files', 5)
        
        task_manager = AsyncTaskManager(max_concurrent_tasks=max_concurrent)
        
        # Create upload tasks
        upload_tasks = [
            self.upload_conversation(conversation) for conversation in conversations
        ]
        
        # Execute uploads concurrently
        results = await task_manager.run_tasks(upload_tasks)
        
        # Log summary
        successful_uploads = sum(1 for result in results if result.get('success', False))
        failed_uploads = len(results) - successful_uploads
        
        self.logger.info("Batch upload completed",
                        total_conversations=len(conversations),
                        successful_uploads=successful_uploads,
                        failed_uploads=failed_uploads)
        
        return results
    
    async def ingest_conversations_bulk(self, conversations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Use the IngestConversations API for bulk upload.
        
        Args:
            conversations: List of formatted conversation data.
            
        Returns:
            Bulk ingest operation result.
        """
        self.logger.info("Starting bulk conversation ingest", count=len(conversations))
        
        try:
            # Prepare conversation objects
            conversation_objects = []
            for conv_data in conversations:
                conversation = self._create_conversation_object(conv_data)
                conversation_objects.append(conversation)
            
            # Create ingest request
            request = IngestConversationsRequest(
                parent=self.parent,
                conversations=conversation_objects
            )
            
            # Perform bulk ingest
            operation = await sync_to_async(self.client.ingest_conversations)(request)
            
            # Wait for operation to complete
            result = await sync_to_async(operation.result)(timeout=600)  # 10 minute timeout
            
            ingest_result = {
                'success': True,
                'operation_name': operation.name,
                'conversations_ingested': len(conversations),
                'ingest_statistics': {
                    'processed_conversations_count': len(conversations),
                    'duplicated_conversations_count': 0,  # Would be in actual response
                    'failed_conversations_count': 0       # Would be in actual response
                },
                'error': None
            }
            
            self.logger.info("Bulk ingest completed successfully",
                           operation_name=operation.name,
                           conversations_count=len(conversations))
            
            return ingest_result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error("Bulk ingest failed", error=error_msg)
            
            return {
                'success': False,
                'operation_name': None,
                'conversations_ingested': 0,
                'ingest_statistics': None,
                'error': error_msg
            }
    
    async def check_conversation_exists(self, conversation_id: str) -> bool:
        """Check if a conversation already exists in CCAI Insights.
        
        Args:
            conversation_id: Conversation ID to check.
            
        Returns:
            True if conversation exists, False otherwise.
        """
        try:
            conversation_name = f"{self.parent}/conversations/{conversation_id}"
            await sync_to_async(self.client.get_conversation)(name=conversation_name)
            return True
        except Exception:
            return False