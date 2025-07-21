"""Data Loss Prevention (DLP) processor for STT E2E Insights."""

import asyncio
from typing import Dict, Any, Optional, List
import json

from google.cloud import dlp_v2
from google.cloud.dlp_v2 import DlpServiceClient, InspectTemplate, DeidentifyTemplate
from google.cloud.dlp_v2.types import ContentItem, InspectConfig, DeidentifyConfig

from utils.logger import LoggerMixin
from utils.config_loader import get_config_section
from utils.async_helpers import sync_to_async, async_retry


class DLPProcessor(LoggerMixin):
    """Handles Data Loss Prevention processing for PII redaction."""
    
    def __init__(self, project_id: Optional[str] = None):
        """Initialize the DLP processor.
        
        Args:
            project_id: GCP project ID. If None, loads from config.
        """
        try:
            gcp_config = get_config_section('gcp')
            self.dlp_config = get_config_section('dlp')
        except KeyError as e:
            raise ValueError(f"Missing configuration section: {e}")
        
        self.project_id = project_id or gcp_config.get('project_id')
        if not self.project_id:
            raise ValueError("GCP project ID must be provided")
        
        self.location = self.dlp_config.get('location', 'global')
        self.identify_template_id = self.dlp_config.get('identify_template_id')
        self.deidentify_template_id = self.dlp_config.get('deidentify_template_id')
        
        # Initialize DLP client
        self.client = DlpServiceClient()
        
        # Build template paths
        self.parent = f"projects/{self.project_id}/locations/{self.location}"
        
        if self.identify_template_id:
            self.inspect_template_path = (
                f"projects/{self.project_id}/locations/{self.location}/"
                f"inspectTemplates/{self.identify_template_id}"
            )
        else:
            self.inspect_template_path = None
        
        if self.deidentify_template_id:
            self.deidentify_template_path = (
                f"projects/{self.project_id}/locations/{self.location}/"
                f"deidentifyTemplates/{self.deidentify_template_id}"
            )
        else:
            self.deidentify_template_path = None
        
        self.logger.info("DLP processor initialized",
                        project_id=self.project_id,
                        location=self.location,
                        identify_template=self.identify_template_id,
                        deidentify_template=self.deidentify_template_id)
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def redact_transcript(self, transcript_text: str, 
                               include_findings: bool = True) -> Dict[str, Any]:
        """Redact PII from transcript text using DLP.
        
        Args:
            transcript_text: The transcript text to process.
            include_findings: Whether to include detailed findings in the result.
            
        Returns:
            Dictionary containing redacted text and optionally findings.
        """
        if not transcript_text or not transcript_text.strip():
            return {
                'original_text': transcript_text,
                'redacted_text': transcript_text,
                'findings': [],
                'redaction_applied': False
            }
        
        self.logger.debug("Starting PII redaction", text_length=len(transcript_text))
        
        # First, inspect the content to identify PII
        findings = []
        if include_findings:
            findings = await self._inspect_content(transcript_text)
        
        # Then, deidentify the content
        redacted_text = await self._deidentify_content(transcript_text)
        
        result = {
            'original_text': transcript_text,
            'redacted_text': redacted_text,
            'findings': findings,
            'redaction_applied': redacted_text != transcript_text,
            'redacted_info_types': list(set([f.info_type for f in findings])) if findings else []
        }
        
        self.logger.info("PII redaction completed",
                        original_length=len(transcript_text),
                        redacted_length=len(redacted_text),
                        findings_count=len(findings),
                        redaction_applied=result['redaction_applied'])
        
        return result
    
    async def _inspect_content(self, text: str) -> List[Dict[str, Any]]:
        """Inspect content for PII using the configured inspect template.
        
        Args:
            text: Text content to inspect.
            
        Returns:
            List of findings.
        """
        if not self.inspect_template_path:
            self.logger.warning("No inspect template configured, skipping inspection")
            return []
        
        # Create content item
        item = ContentItem(value=text)
        
        # Create inspect request
        request = {
            "parent": self.parent,
            "inspect_template": self.inspect_template_path,
            "item": item,
            "include_quote": self.dlp_config.get('include_quote', True)
        }
        
        # Perform inspection
        response = await sync_to_async(self.client.inspect_content)(request)
        
        # Process findings
        findings = []
        if response.result and response.result.findings:
            for finding in response.result.findings:
                finding_data = {
                    'info_type': finding.info_type.name,
                    'likelihood': finding.likelihood.name,
                    'quote': finding.quote if hasattr(finding, 'quote') else None,
                    'location': {
                        'byte_range': {
                            'start': finding.location.byte_range.start,
                            'end': finding.location.byte_range.end
                        } if finding.location and finding.location.byte_range else None
                    }
                }
                findings.append(finding_data)
        
        return findings
    
    async def _deidentify_content(self, text: str) -> str:
        """Deidentify content using the configured deidentify template.
        
        Args:
            text: Text content to deidentify.
            
        Returns:
            Deidentified text.
        """
        if not self.deidentify_template_path:
            self.logger.warning("No deidentify template configured, returning original text")
            return text
        
        # Create content item
        item = ContentItem(value=text)
        
        # Create deidentify request
        request = {
            "parent": self.parent,
            "deidentify_template": self.deidentify_template_path,
            "item": item
        }
        
        # Perform deidentification
        response = await sync_to_async(self.client.deidentify_content)(request)
        
        # Extract deidentified text
        if response.item and hasattr(response.item, 'value'):
            return response.item.value
        
        self.logger.warning("No deidentified content returned, using original text")
        return text
    
    @async_retry(max_attempts=3, delay_seconds=2.0)
    async def redact_conversation_data(self, conversation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Redact PII from entire conversation data structure.
        
        Args:
            conversation_data: Conversation data containing transcripts.
            
        Returns:
            Conversation data with redacted transcripts.
        """
        self.logger.info("Starting conversation data redaction")
        
        redacted_data = conversation_data.copy()
        redaction_summary = {
            'total_fields_processed': 0,
            'fields_redacted': 0,
            'info_types_found': set()
        }
        
        # Redact channel transcripts
        if 'channels' in redacted_data:
            for channel_id, channel_data in redacted_data['channels'].items():
                if 'transcript' in channel_data:
                    redaction_summary['total_fields_processed'] += 1
                    
                    result = await self.redact_transcript(
                        channel_data['transcript'], include_findings=True
                    )
                    
                    channel_data['transcript'] = result['redacted_text']
                    channel_data['original_transcript'] = result['original_text']
                    channel_data['pii_findings'] = result['findings']
                    
                    if result['redaction_applied']:
                        redaction_summary['fields_redacted'] += 1
                    
                    redaction_summary['info_types_found'].update(
                        result.get('redacted_info_types', [])
                    )
        
        # Redact speaker transcripts
        if 'speakers' in redacted_data:
            for speaker_id, speaker_data in redacted_data['speakers'].items():
                if 'transcript' in speaker_data:
                    redaction_summary['total_fields_processed'] += 1
                    
                    result = await self.redact_transcript(
                        speaker_data['transcript'], include_findings=True
                    )
                    
                    speaker_data['transcript'] = result['redacted_text']
                    speaker_data['original_transcript'] = result['original_text']
                    speaker_data['pii_findings'] = result['findings']
                    
                    if result['redaction_applied']:
                        redaction_summary['fields_redacted'] += 1
                    
                    redaction_summary['info_types_found'].update(
                        result.get('redacted_info_types', [])
                    )
        
        # Redact alternative transcripts
        if 'alternatives' in redacted_data:
            for alt_data in redacted_data['alternatives']:
                if 'transcript' in alt_data:
                    redaction_summary['total_fields_processed'] += 1
                    
                    result = await self.redact_transcript(
                        alt_data['transcript'], include_findings=True
                    )
                    
                    alt_data['transcript'] = result['redacted_text']
                    alt_data['original_transcript'] = result['original_text']
                    alt_data['pii_findings'] = result['findings']
                    
                    if result['redaction_applied']:
                        redaction_summary['fields_redacted'] += 1
                    
                    redaction_summary['info_types_found'].update(
                        result.get('redacted_info_types', [])
                    )
        
        # Add redaction metadata
        redacted_data['dlp_metadata'] = {
            'redaction_timestamp': self._get_current_timestamp(),
            'redaction_summary': {
                'total_fields_processed': redaction_summary['total_fields_processed'],
                'fields_redacted': redaction_summary['fields_redacted'],
                'info_types_found': list(redaction_summary['info_types_found'])
            },
            'templates_used': {
                'inspect_template': self.identify_template_id,
                'deidentify_template': self.deidentify_template_id
            }
        }
        
        self.logger.info("Conversation data redaction completed",
                        fields_processed=redaction_summary['total_fields_processed'],
                        fields_redacted=redaction_summary['fields_redacted'],
                        info_types_found=list(redaction_summary['info_types_found']))
        
        return redacted_data
    
    async def batch_redact_conversations(self, 
                                       conversations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Redact PII from multiple conversations concurrently.
        
        Args:
            conversations: List of conversation data to redact.
            
        Returns:
            List of redacted conversation data.
        """
        from ..utils.async_helpers import AsyncTaskManager
        
        processing_config = get_config_section('processing')
        max_concurrent = processing_config.get('max_concurrent_files', 5)
        
        task_manager = AsyncTaskManager(max_concurrent_tasks=max_concurrent)
        
        # Create redaction tasks
        redaction_tasks = [
            self.redact_conversation_data(conversation) 
            for conversation in conversations
        ]
        
        # Execute redactions concurrently
        results = await task_manager.run_tasks(redaction_tasks)
        
        return results
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format.
        
        Returns:
            Current timestamp as ISO string.
        """
        from datetime import datetime
        return datetime.utcnow().isoformat() + 'Z'
    
    async def validate_templates(self) -> Dict[str, bool]:
        """Validate that the configured DLP templates exist and are accessible.
        
        Returns:
            Dictionary indicating template validation status.
        """
        validation_result = {
            'inspect_template_valid': False,
            'deidentify_template_valid': False
        }
        
        # Validate inspect template
        if self.inspect_template_path:
            try:
                await sync_to_async(self.client.get_inspect_template)(
                    name=self.inspect_template_path
                )
                validation_result['inspect_template_valid'] = True
                self.logger.info("Inspect template validation successful",
                               template_path=self.inspect_template_path)
            except Exception as e:
                self.logger.error("Inspect template validation failed",
                                template_path=self.inspect_template_path,
                                error=str(e))
        
        # Validate deidentify template
        if self.deidentify_template_path:
            try:
                await sync_to_async(self.client.get_deidentify_template)(
                    name=self.deidentify_template_path
                )
                validation_result['deidentify_template_valid'] = True
                self.logger.info("Deidentify template validation successful",
                               template_path=self.deidentify_template_path)
            except Exception as e:
                self.logger.error("Deidentify template validation failed",
                                template_path=self.deidentify_template_path,
                                error=str(e))
        
        return validation_result