## Requirements
- Python 3.11 (PySpark 3.5.7 has a known worker-handshake incompatibility with Python 3.12+ on Windows)
- Docker Desktop with WSL2 backend
- `C:\hadoop\bin` must be added to PATH (not just HADOOP_HOME) or Spark Structured Streaming checkpointing fails with `UnsatisfiedLinkError: NativeIO$Windows.access0`