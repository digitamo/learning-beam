# Learning Apache Beam

Learning Apache Beam through Python SDK and Scala/Scio implementations of the same pipelines.

## Project Structure

```
learning-beam/
├── python/                 # Python SDK pipelines
│   ├── src/pipelines/      # Pipeline implementations
│   ├── tests/              # Unit tests
│   └── pyproject.toml      # Poetry config
├── scala/                  # Scala/Scio pipelines
│   ├── src/main/scala/     # Scio implementations
│   └── build.sbt           # sbt config
└── data/                   # Shared sample data
```

## Python

### Setup

```bash
pyenv local learning-beam
source $(pyenv virtualenv-prefix learning-beam)/bin/activate
```

### Running pipelines

```bash
cd python
python src/pipelines/wordcount.py ../data/sample.txt ../data/output
```

### Tests

```bash
cd python
pytest tests/
```

## Scala / Scio

### Running

```bash
cd scala
sbt "runMain learning.beam.BeamIntro"
```

This runs all beam-intro examples (filters, side inputs, Magnolify Cats demo) using the DirectRunner.

### Dependencies

- [Scio](https://spotify.github.io/scio/) 0.14.8 (Spotify's Scala API for Apache Beam)
- [Magnolify](https://github.com/spotify/magnolify) 0.7.4 (type class derivation for case classes)
- Apache Beam 2.59.0 (DirectRunner)
