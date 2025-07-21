# STT E2E Insights Pipeline

A comprehensive end-to-end pipeline for processing speech-to-text (STT) audio files and uploading conversations to Google Cloud Contact Center AI Insights.

## Features

- 🎵 **Audio Processing**: Ingests dual-channel 8,000 Hz Mulaw encoded audio files from Google Cloud Storage
- 🗣️ **Speech-to-Text**: Uses Google Cloud Speech-to-Text API v2 with telephonic model for high-quality transcription
- 🔒 **PII Redaction**: Implements Google Cloud Data Loss Prevention (DLP) API v2 for context-aware PII redaction
- 📊 **CCAI Integration**: Formats and uploads conversations to Contact Center AI Insights
- 🚀 **Async Processing**: Concurrent processing of multiple files for optimal performance
- ⚙️ **Configurable**: YAML-based configuration for easy customization

## Architecture

The pipeline consists of modular components:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   GCS Handler   │    │  STT Processor   │    │  DLP Processor  │
│   (Audio I/O)   │───▶│  (Transcription) │───▶│ (PII Redaction) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
┌─────────────────┐    ┌──────────────────┐             │
│ CCAI Uploader   │◀───│ CCAI Formatter   │◀────────────┘
│ (Insights API)  │    │ (Data Mapping)   │
└─────────────────┘    └──────────────────┘
```

## Project Structure

```
stt-e2e-insights/
├── config/
│   └── config.yaml              # Configuration file
├── src/
│   ├── main.py                  # Main pipeline orchestrator
│   ├── modules/                 # Core processing modules
│   │   ├── gcs_handler.py       # Google Cloud Storage operations
│   │   ├── stt_processor.py     # Speech-to-Text processing
│   │   ├── dlp_processor.py     # Data Loss Prevention
│   │   ├── ccai_formatter.py    # CCAI Insights formatting
│   │   └── ccai_uploader.py     # CCAI Insights upload
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
   - Speech-to-Text API
   - Data Loss Prevention API
   - Contact Center AI Insights API

2. **Authentication**: Set up Google Cloud credentials:
   ```bash
   gcloud auth application-default login
   # OR
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
   ```

3. **DLP Templates**: Create DLP inspect and de-identify templates in your GCP project

4. **GCS Buckets**: 
   - Input bucket for audio files
   - Output bucket for processed data

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

The pipeline processes each audio file through the following steps:

1. **Discovery**: Lists audio files in GCS bucket with specified prefix
2. **Download**: Downloads audio file to temporary local storage
3. **Transcription**: Transcribes audio using Google Cloud STT API
4. **PII Redaction**: Redacts sensitive information using DLP API
5. **Formatting**: Formats data for CCAI Insights
6. **Storage**: Saves processed data back to GCS
7. **Upload**: Uploads conversation to CCAI Insights

### Example Output

```
================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================
Files discovered: 15
Files processed successfully: 14
Files failed: 1
Success rate: 93.33%
Conversations uploaded: 14
Upload success rate: 100.00%
Total duration: 245.67 seconds
```

## Configuration Options

### Speech-to-Text Settings

```yaml
stt:
  language_code: "en-US"
  sample_rate_hertz: 8000
  encoding: "MULAW"
  enable_speaker_diarization: true
  enable_automatic_punctuation: true
  model: "telephony"
  processing_mode: "batch"  # or "streaming"
```

### DLP Settings

```yaml
dlp:
  location: "global"
  identify_template_id: "your-template-id"
  deidentify_template_id: "your-template-id"
  include_quote: true
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

- **Concurrent processing** of multiple files
- **Batch operations** for efficient resource usage
- **Async I/O** for non-blocking operations
- **Configurable rate limiting** to respect API quotas

## Security Considerations

- **PII redaction** using enterprise-grade DLP
- **Secure credential management** via Google Cloud IAM
- **Temporary file cleanup** to prevent data leakage
- **Audit logging** for compliance requirements

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