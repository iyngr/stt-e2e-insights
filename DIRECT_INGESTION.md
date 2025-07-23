# Direct Audio Ingestion with CCAI Insights

This document describes the updated implementation that uses CCAI Insights' built-in STT capabilities through the IngestConversations API.

## Overview

The pipeline now supports **direct audio ingestion** from GCS buckets, eliminating the need for manual STT processing and transcript handling. CCAI Insights automatically processes the audio files using a specified recognizer and handles transcription internally.

## Key Changes

### 1. Removed Hardcoded Project ID
- Project ID is now auto-detected from:
  - `gcloud config get-value project` command
  - `GOOGLE_CLOUD_PROJECT` environment variable
  - `GCP_PROJECT` environment variable
  - GCP metadata service (when running on GCP)

### 2. IngestConversations API Integration
- Uses CCAI Insights' native ingestion endpoint
- Leverages the `ccai-insights-recognizer` for automatic STT
- Handles Long Running Operations (LRO) for completion status
- Processes audio files directly from GCS without local downloads

### 3. Simplified Pipeline
- **Eliminated components**: STT Processor, DLP Processor (for direct ingestion)
- **Streamlined flow**: GCS discovery → Direct ingestion → LRO monitoring
- **Reduced complexity**: No manual transcript processing or formatting

## Configuration

### Updated config.yaml

```yaml
gcp:
  # project_id automatically detected - no need to specify
  service_account_key_path: "path/to/service-account-key.json"

ccai:
  location: "us-central1"
  recognizer_id: "ccai-insights-recognizer"  # Your recognizer name
  conversation_ttl_days: 365
```

### Required Recognizer

The pipeline expects a recognizer with the following resource format:
```
projects/{PROJECT_ID}/locations/us-central1/recognizers/ccai-insights-recognizer
```

Where `PROJECT_ID` is auto-detected from your environment.

## Usage

### Basic Usage
```bash
# Auto-detect project ID from environment
python src/main.py

# Validate setup only
python src/main.py --validate-only

# Process limited number of files
python src/main.py --file-limit 10
```

### Environment Setup
```bash
# Option 1: Use gcloud CLI
gcloud config set project YOUR_PROJECT_ID

# Option 2: Use environment variable
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID

# Option 3: Use GCP_PROJECT variable
export GCP_PROJECT=YOUR_PROJECT_ID
```

## Pipeline Flow

1. **Discovery**: Lists audio files in GCS bucket with specified prefix
2. **URI Conversion**: Converts blob names to GCS URIs (gs://bucket/path)
3. **Direct Ingestion**: Calls IngestConversations API with:
   - Audio GCS URIs
   - Conversation metadata
   - Recognizer specification
4. **LRO Monitoring**: Waits for ingestion completion
5. **Status Reporting**: Reports success/failure with detailed metrics

## Sample Output

```
================================================================================
CCAI INSIGHTS DIRECT INGESTION SUMMARY
================================================================================
Files discovered: 15
Files ingested successfully: 14
Files failed: 1
Success rate: 93.33%
Method: IngestConversations API
Recognizer used: projects/my-project/locations/us-central1/recognizers/ccai-insights-recognizer
LRO completed: True
Operation name: projects/my-project/locations/us-central1/operations/12345
Total duration: 245.67 seconds
```

## Benefits

1. **Simplified Architecture**: Eliminates manual STT processing
2. **Native STT**: Uses CCAI Insights' optimized speech recognition
3. **Automatic PII Handling**: CCAI Insights handles PII detection/redaction internally
4. **Scalability**: Leverages Google's infrastructure for processing
5. **Reduced Latency**: Direct processing without intermediate steps
6. **Cost Optimization**: No duplicate STT API calls

## Testing

Run the comprehensive test suite:
```bash
python test_direct_ingestion.py
```

This validates:
- Configuration loading with project ID auto-detection
- CCAI uploader initialization
- GCS URI generation
- Conversation data structure creation
- Pipeline flow logic

## Migration Notes

If migrating from the previous manual STT approach:

1. **Update config.yaml**: Remove hardcoded `project_id`
2. **Add recognizer**: Ensure the `ccai-insights-recognizer` exists in your project
3. **Set environment**: Configure project ID via environment or gcloud
4. **Test validation**: Run `--validate-only` to verify setup

The new implementation is backward compatible with existing GCS bucket structures and file naming conventions.