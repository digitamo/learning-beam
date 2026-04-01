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
        _ = input | beam.ParDo(self._OutputFn(self.prefix))


with beam.Pipeline() as p:
    _ = (
        p
        | "Create words" >> beam.Create(["Hello Beam", "It`s introduction"])
        | "Log words" >> Output()
    )

    _ = p | "Create numbers" >> beam.Create(range(1, 11)) | "Log numbers" >> Output()
