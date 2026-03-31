#   Licensed to the Apache Software Foundation (ASF) under one


import apache_beam as beam


# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=""):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix + str(element))

    def __init__(self, label=None, prefix=""):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))


with beam.Pipeline() as p:
    input = (
        p
        | "Log lines"
        >> beam.io.ReadFromText("gs://apache-beam-samples/shakespeare/kinglear.txt")
        | beam.Filter(lambda line: line != "")
    )

    _ = (
        input
        | "Log fixed lines" >> beam.combiners.Sample.FixedSizeGlobally(10)
        | beam.FlatMap(lambda sentence: sentence)
        | Output(prefix="Fixed first 10 lines: ")
    )

    words = (
        p
        | "Log words"
        >> beam.io.ReadFromText("gs://apache-beam-samples/shakespeare/kinglear.txt")
        | beam.FlatMap(lambda sentence: sentence.split())
        | beam.Filter(lambda word: not word.isspace() or word.isalnum())
        | beam.combiners.Sample.FixedSizeGlobally(10)
        | beam.FlatMap(lambda word: word)
        | "Log output words" >> Output(prefix="Word: ")
    )
