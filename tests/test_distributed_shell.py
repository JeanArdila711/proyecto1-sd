from dfsha.client.distributed_shell_main import build_client


def test_build_client_returns_distributed_client():
    from dfsha.client.distributed_client import DistributedDFShaClient

    client = build_client(["localhost:1"])
    try:
        assert isinstance(client, DistributedDFShaClient)
    finally:
        client.close()
