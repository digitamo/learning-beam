# Learning Apache Beam

This project is set up to learn Apache Beam with the Python SDK.

## Quick Start

1. **Activate the virtual environment**:
   ```bash
   pyenv local learning-beam
   source $(pyenv virtualenv-prefix learning-beam)/bin/activate
   ```

2. **Run the wordcount example**:
   ```bash
   python src/pipelines/wordcount.py data/sample.txt data/output
   ```

3. **Run tests**:
   ```bash
   pytest tests/
   ```

## Project Structure

```
learning-beam/
├── src/pipelines/      # Your Beam pipelines
├── data/               # Sample data files
├── tests/              # Unit tests
└── pyproject.toml      # Project config
```

## Running Pipelines

### Direct Runner (local)
```bash
python src/pipelines/wordcount.py input.txt output.txt --runner=DirectRunner
```

### With options
```bash
python src/pipelines/wordcount.py input.txt output.txt \
    --runner=DirectRunner \
    --direct_num_workers=2
```

## LazyVim Setup

The following extras are enabled:
- **lang.python**: Python LSP support (pyright + ruff)
- **dap.core**: Debugging support (debugpy)
- **test.core**: Test runner support (pytest)

Keybindings:
- `<leader>dPt` - Debug test method
- `<leader>dPc` - Debug test class
- `<leader>cv` - Select virtual environment

For debugging, ensure you're using the correct Python path in your project.