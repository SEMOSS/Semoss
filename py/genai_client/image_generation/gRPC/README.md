# gRPC

This directory contains the gRPC client for the gRPC server running in a Docker container.

The image_gen_pb2.py and image_gen_pb2_grpc.py files are generated from the image_gen.proto file using the protoc compiler.

You can generate this file by running the following command:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. image_gen.proto
```

The image_gen.proto file should always match the one in the server directory (Docker).

If you make changes to the image_gen.proto file, you will need to regenerate the image_gen_pb2.py and image_gen_pb2_grpc.py files.

You will need to update the import generated in the image_gen_pb2_grpc.py file from the following:

```python
import image_gen_pb2 as image__gen__pb2
```

to:

```python
from genai_client.gRPC import image_gen_pb2 as image__gen__pb2
```

This requires grpcio and grpcio-tools to be installed. You can install them using the following command:

```bash
pip install grpcio grpcio-tools
```