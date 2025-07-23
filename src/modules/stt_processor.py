"""Speech-to-Text processor for STT E2E Insights."""

from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import io

from google.cloud import speech
from google.cloud.speech import SpeechClient, RecognitionConfig, RecognitionAudio
from google.cloud.speech import SpeakerDiarizationConfig

from utils.logger import LoggerMixin
from utils.config_loader import get_config_section


class STTProcessor(LoggerMixin):
    """Handles Speech-to-Text processing using Google Cloud Speech API v2."""
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize the STT processor.
        
        Args:
            project_id: GCP project ID. If None, loads from config.
        """
        try:
            gcp_config = get_config_section('gcp')
            self.stt_config = get_config_section('stt')
        except KeyError as e:
            raise ValueError(f"Missing configuration section: {e}")
        
        self.project_id = project_id or gcp_config.get('project_id')
        if not self.project_id:
            raise ValueError("GCP project ID must be provided")
        
        # Initialize Speech-to-Text client
        self.client = SpeechClient()
        
        self.logger.info("STT processor initialized",
                        project_id=self.project_id,
                        model=self.stt_config.get('model', 'telephony'))
    
    def _create_recognition_config(self) -> RecognitionConfig:
        """Create recognition configuration based on config settings.
        
        Returns:
            Configured RecognitionConfig object.
        """
        # Speaker diarization configuration
        diarization_config = None
        if self.stt_config.get('enable_speaker_diarization', True):
            diarization_config = SpeakerDiarizationConfig(
                enable_speaker_diarization=True,
                min_speaker_count=1,
                max_speaker_count=self.stt_config.get('diarization_speaker_count', 2)
            )
        
        # Create recognition config
        config = RecognitionConfig(
            encoding=getattr(RecognitionConfig.AudioEncoding, 
                           self.stt_config.get('encoding', 'MULAW')),
            sample_rate_hertz=self.stt_config.get('sample_rate_hertz', 8000),
            language_code=self.stt_config.get('language_code', 'en-US'),
            audio_channel_count=self.stt_config.get('audio_channel_count', 2),
            enable_separate_recognition_per_channel=self.stt_config.get(
                'enable_separate_recognition_per_channel', True),
            enable_automatic_punctuation=self.stt_config.get(
                'enable_automatic_punctuation', True),
            enable_word_confidence=self.stt_config.get('enable_word_confidence', True),
            enable_word_time_offsets=self.stt_config.get('enable_word_time_offsets', True),
            model=self.stt_config.get('model', 'telephony'),
            use_enhanced=self.stt_config.get('use_enhanced', True),
            profanity_filter=self.stt_config.get('profanity_filter', False),
            max_alternatives=self.stt_config.get('max_alternatives', 1),
            diarization_config=diarization_config
        )
        
        return config
    
    def transcribe_audio_file(self, gcs_uri: str) -> Dict[str, Any]:
        """Transcribe an audio file using long running recognition.
        
        Args:
            gcs_uri: GCS URI of the audio file to transcribe (e.g., gs://bucket/file.wav).
            
        Returns:
            Dictionary containing transcription results with metadata.
        """
        self.logger.info("Starting audio transcription", gcs_uri=gcs_uri)
        
        # Create recognition audio object with GCS URI
        audio = RecognitionAudio(uri=gcs_uri)
        
        # Create recognition config
        config = self._create_recognition_config()
        
        # Perform long running transcription
        self.logger.debug("Starting long running recognition operation")
        operation = self.client.long_running_recognize(config=config, audio=audio)
        
        # Wait for the operation to complete
        self.logger.info("Waiting for transcription to complete", gcs_uri=gcs_uri)
        response = operation.result(timeout=600)  # 10 minute timeout
        
        # Process and format the response
        result = self._process_transcription_response(response, gcs_uri)
        
        self.logger.info("Audio transcription completed",
                        gcs_uri=gcs_uri,
                        alternatives_count=len(result.get('alternatives', [])))
        
        return result

    def _process_transcription_response(self, response: Any, 
                                      gcs_uri: str) -> Dict[str, Any]:
        """Process and format the transcription response.
        
        Args:
            response: Recognition response from the API.
            gcs_uri: Original GCS URI of the audio file.
            
        Returns:
            Formatted transcription result.
        """
        result = {
            'gcs_uri': gcs_uri,
            'file_name': Path(gcs_uri).name,
            'alternatives': [],
            'channels': {},
            'speakers': {},
            'metadata': {
                'total_duration': 0.0,
                'confidence_avg': 0.0,
                'word_count': 0
            }
        }
        
        total_confidence = 0.0
        total_words = 0
        max_end_time = 0.0
        
        # Process each recognition result
        for i, recognition_result in enumerate(response.results):
            if not recognition_result.alternatives:
                continue
            
            # Get the best alternative
            alternative = recognition_result.alternatives[0]
            
            # Extract channel information
            channel = getattr(recognition_result, 'channel_tag', 0)
            if channel not in result['channels']:
                result['channels'][channel] = {
                    'transcript': '',
                    'words': [],
                    'confidence': 0.0
                }
            
            # Process words with timing and confidence
            words_data = []
            channel_confidence = 0.0
            channel_word_count = 0
            
            if hasattr(alternative, 'words'):
                for word in alternative.words:
                    word_data = {
                        'word': word.word,
                        'start_time': word.start_time.total_seconds() if word.start_time else 0.0,
                        'end_time': word.end_time.total_seconds() if word.end_time else 0.0,
                        'confidence': word.confidence if hasattr(word, 'confidence') else 0.0,
                        'speaker_tag': getattr(word, 'speaker_tag', 0)
                    }
                    words_data.append(word_data)
                    
                    # Update duration
                    if word_data['end_time'] > max_end_time:
                        max_end_time = word_data['end_time']
                    
                    # Update confidence tracking
                    channel_confidence += word_data['confidence']
                    channel_word_count += 1
                    
                    # Group by speaker
                    speaker_tag = word_data['speaker_tag']
                    if speaker_tag not in result['speakers']:
                        result['speakers'][speaker_tag] = {
                            'transcript': '',
                            'words': [],
                            'confidence': 0.0
                        }
                    result['speakers'][speaker_tag]['words'].append(word_data)
            
            # Update channel data
            result['channels'][channel]['transcript'] += alternative.transcript + ' '
            result['channels'][channel]['words'].extend(words_data)
            if channel_word_count > 0:
                result['channels'][channel]['confidence'] = channel_confidence / channel_word_count
            
            # Add to alternatives
            alternative_data = {
                'transcript': alternative.transcript,
                'confidence': alternative.confidence,
                'channel': channel,
                'words': words_data
            }
            result['alternatives'].append(alternative_data)
            
            # Update global stats
            total_confidence += alternative.confidence
            total_words += channel_word_count
        
        # Clean up transcripts
        for channel_data in result['channels'].values():
            channel_data['transcript'] = channel_data['transcript'].strip()
        
        # Build speaker transcripts
        for speaker_tag, speaker_data in result['speakers'].items():
            words = sorted(speaker_data['words'], key=lambda x: x['start_time'])
            speaker_data['transcript'] = ' '.join([w['word'] for w in words])
            if words:
                speaker_data['confidence'] = sum(w['confidence'] for w in words) / len(words)
        
        # Update metadata
        result['metadata']['total_duration'] = max_end_time
        if len(result['alternatives']) > 0:
            result['metadata']['confidence_avg'] = total_confidence / len(result['alternatives'])
        result['metadata']['word_count'] = total_words
        
        return result
    
    def batch_transcribe_files(self, gcs_uris: List[str]) -> List[Dict[str, Any]]:
        """Transcribe multiple audio files.
        
        Args:
            gcs_uris: List of GCS URIs to transcribe.
            
        Returns:
            List of transcription results.
        """
        results = []
        for gcs_uri in gcs_uris:
            try:
                result = self.transcribe_audio_file(gcs_uri)
                results.append(result)
            except Exception as e:
                self.logger.error("Failed to transcribe file", gcs_uri=gcs_uri, error=str(e))
                results.append({
                    'gcs_uri': gcs_uri,
                    'error': str(e),
                    'success': False
                })
        return results
    
    def get_channel_transcript(self, transcription_result: Dict[str, Any], 
                              channel: int) -> str:
        """Extract transcript for a specific channel.
        
        Args:
            transcription_result: Result from transcribe_audio_file.
            channel: Channel number (0 for customer, 1 for agent typically).
            
        Returns:
            Transcript text for the specified channel.
        """
        channels = transcription_result.get('channels', {})
        if channel in channels:
            return channels[channel]['transcript']
        return ""
    
    def get_speaker_transcript(self, transcription_result: Dict[str, Any], 
                              speaker: int) -> str:
        """Extract transcript for a specific speaker.
        
        Args:
            transcription_result: Result from transcribe_audio_file.
            speaker: Speaker tag number.
            
        Returns:
            Transcript text for the specified speaker.
        """
        speakers = transcription_result.get('speakers', {})
        if speaker in speakers:
            return speakers[speaker]['transcript']
        return ""