# STT E2E Insights Pipeline

A comprehensive end-to-end pipeline for processing speech-to-text (STT) audio files and uploading conversations to Google Cloud Contact Center AI Insights.

## Features

- 🎵 **Direct Audio Ingestion**: Processes audio files directly through CCAI Insights IngestConversations API
- 🗣️ **Built-in Speech Recognition**: Leverages CCAI's internal speech recognizer for high-quality transcription
- 🔒 **Optional PII Redaction**: Supports DLP templates for sensitive data protection during ingestion
- 📊 **CCAI Integration**: Seamless conversation upload to Contact Center AI Insights with proper metadata
- 🚀 **Efficient Processing**: Bulk processing with built-in file discovery and duplicate handling
- ⚙️ **Configurable**: YAML-based configuration for easy customization

## Architecture

The pipeline uses direct audio ingestion with CCAI Insights:

```
┌─────────────────┐    ┌──────────────────┐
│   GCS Handler   │    │ CCAI Uploader    │
│   (Audio I/O)   │───▶│ (Direct Ingestion│
└─────────────────┘    │  with Recognizer)│
                       └──────────────────┘
```

The CCAI Insights service handles STT processing internally using the configured recognizer, eliminating the need for separate transcription and formatting steps.

## Project Structure

```
stt-e2e-insights/
├── config/
│   └── config.yaml              # Configuration file
├── src/
│   ├── main.py                  # Main pipeline orchestrator
│   ├── modules/                 # Core processing modules
│   │   ├── gcs_handler.py       # Google Cloud Storage operations
│   │   └── ccai_uploader.py     # CCAI Insights direct ingestion
│   └── utils/                   # Utility modules
│       ├── config_loader.py     # Configuration management
│       ├── logger.py            # Structured logging
│       └── async_helpers.py     # Async utilities
├── requirements.txt             # Python dependencies
├── setup.py                     # Package setup
└── README.md                    # This file
```

## Prerequisites

1. **Google Cloud Project** with the following APIs enabled:

   - Cloud Storage API
   - Contact Center AI Insights API
   - Data Loss Prevention API (optional, for PII redaction)

2. **Authentication**: Set up Google Cloud credentials:

   ```bash
   gcloud auth application-default login
   # OR
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
   ```

3. **CCAI Insights Speech Recognizer**: Create a speech recognizer in CCAI Insights for your project/location

4. **DLP Templates** (optional): Create DLP inspect and de-identify templates for PII redaction

5. **GCS Buckets**:
   - Input bucket containing audio files for ingestion

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd stt-e2e-insights
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Install the package (optional):
   ```bash
   pip install -e .
   ```

## Configuration

Copy and customize the configuration file:

```bash
cp config/config.yaml config/config.yaml.local
```

Update the following key settings in `config/config.yaml`:

```yaml
gcp:
  project_id: "your-gcp-project-id"

gcs:
  input_bucket: "your-input-bucket"
  output_bucket: "your-output-bucket"
  file_prefix_filter: "merged"

dlp:
  identify_template_id: "your-identify-template-id"
  deidentify_template_id: "your-deidentify-template-id"

ccai:
  location: "us-central1"
```

## Usage

### Basic Usage

Run the complete pipeline:

```bash
python src/main.py
```

### Advanced Usage

```bash
# Use custom config file
python src/main.py --config config/config.yaml.local

# Validate setup only
python src/main.py --validate-only

# Process limited number of files
python src/main.py --file-limit 10
```

### Pipeline Steps

The pipeline processes audio files through direct ingestion:

1. **Discovery**: Lists audio files in GCS bucket with specified prefix
2. **Direct Ingestion**: Uses CCAI Insights IngestConversations API for bulk processing
3. **Built-in Processing**: CCAI handles speech recognition, formatting, and storage internally
4. **Optional DLP**: Applies redaction templates if configured
5. **Monitoring**: Tracks ingestion operation and reports results

### Example Output

```
================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================
Files discovered: 15
Files newly ingested: 12
Files skipped (duplicates): 2
Files failed: 1
Success rate: 93.33%
Total processed by API: 14
LRO completed: true
```

Total duration: 245.67 seconds

````

## Configuration Options

### DLP Settings (Optional)

```yaml
dlp:
  location: "us-central1"
  identify_template_id: "your-template-id"
  deidentify_template_id: "your-template-id"
  include_quote: true
````

### CCAI Settings

```yaml
ccai:
  location: "us-central1"
  recognizer_id: "your-recognizer-name"
  conversation_ttl_days: 365
  agent_id: "agent-001"
  customer_channel: 1
  agent_channel: 2
```

### Processing Settings

```yaml
processing:
  max_concurrent_files: 5
  retry_attempts: 3
  retry_delay_seconds: 2
```

## Monitoring and Logging

The pipeline provides comprehensive logging:

- **Structured logging** with JSON format
- **Log levels**: DEBUG, INFO, WARNING, ERROR
- **Log destinations**: Console and file output
- **Performance metrics**: Processing times and success rates

Logs are written to `logs/stt_insights.log` by default.

## Error Handling

The pipeline includes robust error handling:

- **Retry logic** for transient failures
- **Graceful degradation** when optional features fail
- **Detailed error reporting** for troubleshooting
- **Temporary file cleanup** after processing

## Performance Optimization

- **Direct ingestion** eliminates intermediate processing steps
- **Bulk operations** for efficient resource usage
- **Built-in file discovery** by CCAI API reduces overhead
- **Configurable rate limiting** to respect API quotas
- **Duplicate detection** prevents reprocessing of existing conversations

## Security Considerations

- **Optional PII redaction** using enterprise-grade DLP templates
- **Secure credential management** via Google Cloud IAM
- **Direct processing** eliminates temporary file storage
- **Audit logging** for compliance requirements
- **Project-based isolation** for multi-tenant deployments

## Troubleshooting

### Common Issues

1. **Authentication errors**: Ensure Google Cloud credentials are properly configured
2. **Permission errors**: Verify IAM permissions for all required APIs
3. **Template not found**: Confirm DLP template IDs are correct
4. **Bucket access**: Check GCS bucket permissions and names

### Debug Mode

Enable debug logging for detailed troubleshooting:

```yaml
logging:
  level: "DEBUG"
```

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=src
```

### Code Quality

```bash
# Format code
black src/
isort src/

# Lint code
flake8 src/

# Type checking
mypy src/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions and support:

1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue in the repository
4. Contact the development team

## Changelog

### Version 1.0.0

- Initial release
- Complete STT E2E pipeline implementation
- Support for dual-channel audio processing
- DLP integration for PII redaction
- CCAI Insights integration
- Async processing capabilities
