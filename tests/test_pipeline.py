import pytest


def test_beam_import():
    import apache_beam as beam
    assert beam.__version__ == "2.71.0"


def test_wordcount_basic():
    from pipelines.wordcount import run_wordcount
    
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("hello world hello\n")
        input_path = f.name
    
    output_dir = tempfile.mkdtemp()
    output_path = os.path.join(output_dir, "output")
    
    try:
        run_wordcount(input_path, output_path)
        
        with open(f"{output_path}-00000-of-00001.txt", 'r') as f:
            content = f.read()
            assert "hello: 2" in content
            assert "world: 1" in content
    finally:
        os.unlink(input_path)
        for f in os.listdir(output_dir):
            os.unlink(os.path.join(output_dir, f))
        os.rmdir(output_dir)