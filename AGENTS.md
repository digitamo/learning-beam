# Learning Beam

A learning project for Apache Beam with Python SDK and Scala/Scio implementations side by side.

## Layout

- `python/` — Python pipelines (Poetry, pyright, pytest)
- `scala/` — Scala 2.13 pipelines (sbt, Scio, Magnolify)
- `data/` — shared sample data used by both

## Python

- Managed with Poetry (`python/pyproject.toml`)
- Run pipelines: `cd python && python src/pipelines/<name>.py`
- Run tests: `cd python && pytest tests/`

## Scala

- Built with sbt (`scala/build.sbt`)
- Scio 0.14.8, Magnolify-cats 0.7.4, Beam 2.59.0
- Run: `cd scala && sbt "runMain learning.beam.BeamIntro"`
- Compile check: `cd scala && sbt compile`

## Conventions

- Each Python notebook/pipeline should have a corresponding Scala translation under `scala/src/main/scala/learning/beam/`
- Scala code uses case classes instead of Python dicts for data types
- Comments in Scala files explain the Python-to-Scio translation
