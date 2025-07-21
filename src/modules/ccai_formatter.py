"""CCAI Insights formatter for STT E2E Insights."""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import uuid

from utils.logger import LoggerMixin
from utils.config_loader import get_config_section


class CCAIFormatter(LoggerMixin):
    """Formats transcribed and redacted data for CCAI Insights."""
    
    def __init__(self):
        """Initialize the CCAI formatter."""
        try:
            self.ccai_config = get_config_section('ccai')
        except KeyError as e:
            raise ValueError(f"Missing configuration section: {e}")
        
        self.logger.info("CCAI formatter initialized")
    
    async def format_conversation(self, 
                                transcription_data: Dict[str, Any],
                                redacted_data: Dict[str, Any],
                                audio_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Format transcription and redacted data for CCAI Insights.
        
        Args:
            transcription_data: Output from STT processor.
            redacted_data: Output from DLP processor.
            audio_metadata: Optional metadata about the original audio file.
            
        Returns:
            Formatted conversation data for CCAI Insights.
        """
        self.logger.debug("Formatting conversation for CCAI Insights",
                         file_name=transcription_data.get('file_name'))
        
        # Generate conversation metadata
        conversation_id = self._generate_conversation_id(transcription_data.get('file_name'))
        
        # Build the conversation structure
        conversation = {
            "name": f"projects/{self.ccai_config.get('location', 'us-central1')}/conversations/{conversation_id}",
            "data_source": {
                "dialogflow_source": {
                    "audio_uri": audio_metadata.get('gcs_uri') if audio_metadata else None,
                    "dialogflow_conversation": None
                }
            },
            "medium": "PHONE_CALL",
            "call_metadata": self._build_call_metadata(transcription_data, audio_metadata),
            "expire_time": self._calculate_expire_time(),
            "ttl": f"{self.ccai_config.get('conversation_ttl_days', 365)}d",
            "language_code": transcription_data.get('language_code', 'en-US'),
            "conversation_transcript": self._build_conversation_transcript(redacted_data),
            "runtime_annotations": self._build_runtime_annotations(transcription_data, redacted_data)
        }
        
        self.logger.info("Conversation formatted successfully",
                        conversation_id=conversation_id,
                        transcript_length=len(conversation['conversation_transcript']['transcript_segments']))
        
        return conversation
    
    def _generate_conversation_id(self, file_name: str = None) -> str:
        """Generate a unique conversation ID.
        
        Args:
            file_name: Optional file name to incorporate into ID.
            
        Returns:
            Unique conversation ID.
        """
        # Use file name (without extension) and timestamp for unique ID
        if file_name:
            base_name = file_name.split('.')[0]
            # Remove non-alphanumeric characters except hyphens and underscores
            clean_name = ''.join(c for c in base_name if c.isalnum() or c in '-_')
            timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
            return f"{clean_name}-{timestamp}"
        else:
            # Fallback to UUID
            return str(uuid.uuid4())
    
    def _build_call_metadata(self, 
                           transcription_data: Dict[str, Any], 
                           audio_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build call metadata for the conversation.
        
        Args:
            transcription_data: Transcription data.
            audio_metadata: Audio file metadata.
            
        Returns:
            Call metadata dictionary.
        """
        duration_seconds = transcription_data.get('metadata', {}).get('total_duration', 0.0)
        
        call_metadata = {
            "customer_channel": 0,  # Assuming channel 0 is customer
            "agent_channel": 1,     # Assuming channel 1 is agent
            "agent_id": self.ccai_config.get('agent_id', 'agent-001'),
            "customer_id": self.ccai_config.get('customer_id', 'customer-001')
        }
        
        # Add duration if available
        if duration_seconds > 0:
            call_metadata["call_duration"] = f"{int(duration_seconds)}s"
        
        return call_metadata
    
    def _calculate_expire_time(self) -> str:
        """Calculate conversation expiration time.
        
        Returns:
            Expiration time in RFC3339 format.
        """
        from datetime import timedelta
        
        ttl_days = self.ccai_config.get('conversation_ttl_days', 365)
        expire_time = datetime.utcnow() + timedelta(days=ttl_days)
        return expire_time.replace(tzinfo=timezone.utc).isoformat()
    
    def _build_conversation_transcript(self, redacted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build the conversation transcript structure.
        
        Args:
            redacted_data: Redacted transcription data.
            
        Returns:
            Conversation transcript structure.
        """
        transcript_segments = []
        
        # Process channel-based segments
        channels = redacted_data.get('channels', {})
        for channel_id, channel_data in channels.items():
            if 'words' in channel_data:
                segments = self._create_segments_from_words(
                    channel_data['words'], 
                    channel_id, 
                    participant_role="CUSTOMER" if channel_id == 0 else "AGENT"
                )
                transcript_segments.extend(segments)
        
        # If no channel data, try speaker-based segments
        if not transcript_segments:
            speakers = redacted_data.get('speakers', {})
            for speaker_id, speaker_data in speakers.items():
                if 'words' in speaker_data:
                    segments = self._create_segments_from_words(
                        speaker_data['words'], 
                        speaker_id,
                        participant_role="CUSTOMER" if speaker_id == 0 else "AGENT"
                    )
                    transcript_segments.extend(segments)
        
        # Sort segments by start time
        transcript_segments.sort(key=lambda x: float(x.get('segment_start_time', '0.0').rstrip('s')))
        
        return {
            "transcript_segments": transcript_segments
        }
    
    def _create_segments_from_words(self, 
                                  words: List[Dict[str, Any]], 
                                  channel_or_speaker: int,
                                  participant_role: str) -> List[Dict[str, Any]]:
        """Create transcript segments from word-level data.
        
        Args:
            words: List of word dictionaries with timing information.
            channel_or_speaker: Channel or speaker identifier.
            participant_role: Role of the participant (CUSTOMER/AGENT).
            
        Returns:
            List of transcript segments.
        """
        if not words:
            return []
        
        segments = []
        
        # Group words into sentences/segments based on natural breaks
        current_segment = {
            'words': [],
            'start_time': None,
            'end_time': None
        }
        
        for word_data in words:
            # Add word to current segment
            current_segment['words'].append(word_data)
            
            # Set start time for first word
            if current_segment['start_time'] is None:
                current_segment['start_time'] = word_data.get('start_time', 0.0)
            
            # Update end time
            current_segment['end_time'] = word_data.get('end_time', 0.0)
            
            # Check for natural breaks (punctuation, pauses, etc.)
            word_text = word_data.get('word', '')
            if (word_text.endswith('.') or word_text.endswith('?') or word_text.endswith('!') or
                len(current_segment['words']) >= 20):  # Max 20 words per segment
                
                # Finalize current segment
                if current_segment['words']:
                    segment = self._create_transcript_segment(current_segment, participant_role)
                    segments.append(segment)
                
                # Reset for next segment
                current_segment = {
                    'words': [],
                    'start_time': None,
                    'end_time': None
                }
        
        # Add any remaining words as final segment
        if current_segment['words']:
            segment = self._create_transcript_segment(current_segment, participant_role)
            segments.append(segment)
        
        return segments
    
    def _create_transcript_segment(self, 
                                 segment_data: Dict[str, Any], 
                                 participant_role: str) -> Dict[str, Any]:
        """Create a single transcript segment.
        
        Args:
            segment_data: Segment data with words, start_time, end_time.
            participant_role: Role of the participant.
            
        Returns:
            Formatted transcript segment.
        """
        # Build segment text
        segment_text = ' '.join([word.get('word', '') for word in segment_data['words']])
        
        # Calculate confidence
        confidences = [word.get('confidence', 0.0) for word in segment_data['words'] 
                      if word.get('confidence', 0.0) > 0]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        
        # Build word-level info
        words_info = []
        for word_data in segment_data['words']:
            word_info = {
                "word": word_data.get('word', ''),
                "start_offset": f"{word_data.get('start_time', 0.0)}s",
                "end_offset": f"{word_data.get('end_time', 0.0)}s",
                "confidence": word_data.get('confidence', 0.0)
            }
            words_info.append(word_info)
        
        segment = {
            "text": segment_text,
            "confidence": avg_confidence,
            "words": words_info,
            "segment_start_time": f"{segment_data['start_time']}s",
            "segment_end_time": f"{segment_data['end_time']}s",
            "segment_participant": {
                "dialogflow_participant_name": None,
                "obfuscated_external_user_id": f"{participant_role.lower()}-001",
                "role": participant_role
            },
            "language_code": "en-US",
            "channel_tag": segment_data.get('channel', 0)
        }
        
        return segment
    
    def _build_runtime_annotations(self, 
                                 transcription_data: Dict[str, Any],
                                 redacted_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build runtime annotations for the conversation.
        
        Args:
            transcription_data: Original transcription data.
            redacted_data: Redacted data with DLP information.
            
        Returns:
            List of runtime annotations.
        """
        annotations = []
        
        # Add DLP annotation if redaction was applied
        dlp_metadata = redacted_data.get('dlp_metadata', {})
        redaction_summary = dlp_metadata.get('redaction_summary', {})
        
        if redaction_summary.get('fields_redacted', 0) > 0:
            dlp_annotation = {
                "annotation_id": str(uuid.uuid4()),
                "create_time": dlp_metadata.get('redaction_timestamp', 
                                              datetime.utcnow().isoformat() + 'Z'),
                "annotation_type": "DATA_REDACTION",
                "annotation_payload": {
                    "redaction_summary": redaction_summary,
                    "templates_used": dlp_metadata.get('templates_used', {})
                }
            }
            annotations.append(dlp_annotation)
        
        # Add quality metrics annotation
        metadata = transcription_data.get('metadata', {})
        if metadata:
            quality_annotation = {
                "annotation_id": str(uuid.uuid4()),
                "create_time": datetime.utcnow().isoformat() + 'Z',
                "annotation_type": "QUALITY_METRICS",
                "annotation_payload": {
                    "transcript_quality": {
                        "average_confidence": metadata.get('confidence_avg', 0.0),
                        "total_duration": metadata.get('total_duration', 0.0),
                        "word_count": metadata.get('word_count', 0)
                    }
                }
            }
            annotations.append(quality_annotation)
        
        return annotations
    
    async def batch_format_conversations(self, 
                                       transcription_results: List[Dict[str, Any]],
                                       redaction_results: List[Dict[str, Any]],
                                       audio_metadata_list: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Format multiple conversations concurrently.
        
        Args:
            transcription_results: List of transcription results.
            redaction_results: List of redaction results.
            audio_metadata_list: Optional list of audio metadata.
            
        Returns:
            List of formatted conversations.
        """
        from ..utils.async_helpers import AsyncTaskManager
        
        if len(transcription_results) != len(redaction_results):
            raise ValueError("Transcription and redaction results lists must have the same length")
        
        audio_metadata_list = audio_metadata_list or [{}] * len(transcription_results)
        
        task_manager = AsyncTaskManager(max_concurrent_tasks=5)
        
        # Create formatting tasks
        formatting_tasks = [
            self.format_conversation(
                transcription_results[i],
                redaction_results[i],
                audio_metadata_list[i] if i < len(audio_metadata_list) else {}
            )
            for i in range(len(transcription_results))
        ]
        
        # Execute formatting concurrently
        results = await task_manager.run_tasks(formatting_tasks)
        
        return results
    
    def create_conversation_metadata(self, 
                                   formatted_conversation: Dict[str, Any]) -> Dict[str, Any]:
        """Create metadata summary for a formatted conversation.
        
        Args:
            formatted_conversation: Formatted conversation data.
            
        Returns:
            Metadata summary.
        """
        transcript = formatted_conversation.get('conversation_transcript', {})
        segments = transcript.get('transcript_segments', [])
        
        metadata = {
            'conversation_id': formatted_conversation.get('name', '').split('/')[-1],
            'medium': formatted_conversation.get('medium'),
            'language_code': formatted_conversation.get('language_code'),
            'segment_count': len(segments),
            'total_words': sum(len(seg.get('words', [])) for seg in segments),
            'call_metadata': formatted_conversation.get('call_metadata', {}),
            'annotations_count': len(formatted_conversation.get('runtime_annotations', []))
        }
        
        return metadata