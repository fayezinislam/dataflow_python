# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import time
import logging
import json
import typing
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.runners import DataflowRunner, DirectRunner
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub

# Helper Functions and Classes

class ParseJsonWithDLQ(beam.DoFn):
    """
    Parses a JSON byte string into a Python dictionary.
    Outputs successfully parsed messages to the main output tag.
    Outputs messages that fail parsing to the 'dead_letter_data' tag.
    """
    SUCCESS_TAG = 'parsed_data'
    FAILURE_TAG = 'dead_letter_data'

    def process(self, element: bytes):
        try:
            parsed_dict = json.loads(element)
            yield TaggedOutput(self.SUCCESS_TAG, parsed_dict)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}. Message: {element[:100]}...", exc_info=False)
            yield TaggedOutput(self.FAILURE_TAG, element)
        except Exception as e:
            logging.error(f"An unexpected error occurred during parsing: {e}. Message: {element[:100]}...", exc_info=True)
            yield TaggedOutput(self.FAILURE_TAG, element)

def add_processing_timestamp(element: dict) -> dict:
    """Adds processing timestamps to a dictionary."""
    now_utc = datetime.now(timezone.utc)
    element['processingTimestamp'] = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return element

def encode_to_json_bytes(element: dict) -> bytes:
    """Encodes a dictionary to a JSON formatted byte string."""
    return json.dumps(element).encode('utf-8')

class RemoveFieldsFn(beam.DoFn):
    """Removes specified fields from a dictionary element."""
    def __init__(self, fields_to_remove_set):
        if isinstance(fields_to_remove_set, str):
             self.fields_to_remove = set(f.strip() for f in fields_to_remove_set.split(',') if f.strip())
        elif isinstance(fields_to_remove_set, (list, set)):
             self.fields_to_remove = set(fields_to_remove_set)
        else:
             self.fields_to_remove = set()
        logging.info(f"RemoveFieldsFn initialized. Fields to remove: {self.fields_to_remove}")

    def process(self, element: dict):
        if not self.fields_to_remove:
            yield element
            return
        element_copy = element.copy()
        for field in self.fields_to_remove:
            element_copy.pop(field, None) # Use pop with default None to avoid KeyError if field doesn't exist
        yield element_copy

# Dataflow pipeline run method
def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub, process events, write successes and failures to Pub/Sub')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner (e.g., DataflowRunner, DirectRunner)')
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub Topic (full path: projects/<PROJECT>/topics/<TOPIC>)')
    parser.add_argument('--output_topic', required=True, help='Main output Pub/Sub Topic for successfully processed events (full path: projects/<PROJECT>/topics/<TOPIC>)')
    parser.add_argument('--dead_letter_topic', required=True, help='Output Pub/Sub Topic for messages that failed processing (full path: projects/<PROJECT>/topics/<TOPIC>)')
    parser.add_argument('--fields_to_remove', type=str, default=None, help='Comma-separated list of fields to remove from the main output (e.g., "user_id,ip")')
    parser.add_argument('--num_workers', type=int, default=None, help='Initial number of workers for the job')
    parser.add_argument('--max_num_workers', type=int, default=None, help='Max number of workers for the job')
    parser.add_argument('--network', type=str, default=None, help='VPC network name')
    parser.add_argument('--subnetwork', type=str, default=None, help='Subnetwork name')

    opts = parser.parse_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(save_main_session=True, streaming=True, pipeline_options=[
        f"--network={opts.network}",
        f"--subnetwork={opts.subnetwork}"
    ]) # Make sure streaming is True
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = opts.project
    google_cloud_options.region = opts.region
    google_cloud_options.staging_location = opts.staging_location
    google_cloud_options.temp_location = opts.temp_location
    google_cloud_options.job_name = '{0}{1}'.format('df-pubsub-streaming-redaction-pipeline-',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner
    options.view_as(WorkerOptions).num_workers = opts.num_workers
    options.view_as(WorkerOptions).max_num_workers = opts.max_num_workers

    input_topic = opts.input_topic
    output_topic = opts.output_topic
    dead_letter_topic = opts.dead_letter_topic

    # Process fields_to_remove Argument
    fields_to_remove_set = set()
    if opts.fields_to_remove:
        fields_to_remove_set = set(f.strip() for f in opts.fields_to_remove.split(',') if f.strip())

    # Create the pipeline
    p = beam.Pipeline(options=options)

    # Read from Pub/Sub
    raw_msgs = (p | 'ReadFromPubSub' >> ReadFromPubSub(topic=input_topic).with_output_types(bytes))

    # Parse JSON with Dead Letter Queue logic
    parse_results = (raw_msgs | 'ParseJsonWithDLQ' >> beam.ParDo(ParseJsonWithDLQ()).with_outputs(ParseJsonWithDLQ.SUCCESS_TAG, ParseJsonWithDLQ.FAILURE_TAG))

    # Handle Dead Letter Output
    dead_letter_msgs = parse_results[ParseJsonWithDLQ.FAILURE_TAG]
    (dead_letter_msgs | 'WriteDeadLetterToPubSub' >> WriteToPubSub(topic=dead_letter_topic))

    # Handle Successful Output
    parsed_msgs = parse_results[ParseJsonWithDLQ.SUCCESS_TAG]

    # Continue processing successfully parsed messages
    processed_events = (parsed_msgs | "AddProcessingTimestamp" >> beam.Map(add_processing_timestamp))

    # Conditionally add the RemoveFieldsFn step
    if fields_to_remove_set:
        logging.info(f"Adding step to remove fields: {fields_to_remove_set}")
        processed_events = (processed_events | 'RemoveSpecifiedFields' >> beam.ParDo(RemoveFieldsFn(fields_to_remove_set)))
    else:
         logging.info("No fields specified for removal.")

    # Encode dictionary to JSON bytes and write successfully processed events to Pub/Sub
    (processed_events # Use the final processed PCollection
        | "EncodeSuccessToJson" >> beam.Map(encode_to_json_bytes) # Changed label for clarity
        | 'WriteSuccessToPubSub' >> WriteToPubSub(topic=output_topic)
     )
    # End Handle Successful Output ---

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline...")
    # Using current time: Monday, April 7, 2025 at 3:26:02 PM EDT (Example Time)
    logging.info(f"Current time: {datetime.now().isoformat()}") # Log actual current time

    # Run the pipeline
    # For streaming pipelines, this initiates the job. Use wait_until_finish()
    # only if you are running a batch job or want the script to block until
    # the streaming job fails or is manually cancelled. Typically for streaming,
    # you launch it and monitor elsewhere (like the GCP console).
    pipeline_result = p.run()
    # For a long-running streaming job, you might not want to wait here.
    # pipeline_result.wait_until_finish() # Comment out for typical streaming deployment

    logging.info(f"Pipeline job started. Check the Dataflow UI: https://console.cloud.google.com/dataflow?project={opts.project}")


if __name__ == '__main__':
  run()
