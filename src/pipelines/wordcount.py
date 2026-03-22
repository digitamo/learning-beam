import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.combiners as beam_combiners
import argparse
import logging

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="Input file to process")
    parser.add_argument("--output", required=True, help="Output file path")

    known_args, beam_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(beam_args)

    input_file = known_args.input_file
    output_file = known_args.output

    with beam.Pipeline(options=pipeline_options) as p:
        _ = (
            p
            | "ReadInput" >> ReadFromText(input_file)
            | "SplitWords" >> beam.FlatMap(lambda line: line.split())
            | "CountWords" >> beam_combiners.Count.PerElement()
            | "FormatOutput" >> beam.Map(lambda x: f"{x[0]}: {x[1]}")
            | "WriteOutput" >> WriteToText(output_file)
        )

    result = p.run()
    result.wait_until_finish()
