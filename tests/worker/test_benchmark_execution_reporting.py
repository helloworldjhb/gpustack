from types import SimpleNamespace
from unittest.mock import Mock

from gpustack.worker import benchmark_manager as manager
from gpustack.worker.benchmark.runner import BenchmarkRunner


def test_result_posts_are_bound_to_their_execution(monkeypatch):
    client = Mock()
    monkeypatch.setattr(manager, 'raise_if_response_error', lambda response: None)
    instance = object.__new__(manager.BenchmarkManager)
    instance._clientset_getter = lambda: SimpleNamespace(
        http_client=SimpleNamespace(get_httpx_client=lambda: client)
    )
    benchmark = SimpleNamespace(id=7, name='bench', run_id='execution-a')
    instance._post_metrics_and_results(
        benchmark, SimpleNamespace(model_dump=lambda: {}), []
    )
    assert [call.args[0] for call in client.post.call_args_list] == [
        '/benchmarks/7/metrics?run_id=execution-a',
        '/benchmarks/7/results?run_id=execution-a',
    ]


def test_runner_failure_patch_includes_execution_identity():
    client = Mock()
    runner = object.__new__(BenchmarkRunner)
    runner._benchmark = SimpleNamespace(run_id='execution-a')
    runner._clientset = SimpleNamespace(
        http_client=SimpleNamespace(get_httpx_client=lambda: client)
    )
    runner._update_benchmark_state(7, state='error')
    assert client.patch.call_args.kwargs['json']['run_id'] == 'execution-a'


def test_retired_writer_ack_releases_local_execution():
    response = Mock(status_code=409)
    response.json.return_value = {'details': {'code': 'benchmark_run_changed'}}
    assert manager.BenchmarkManager._state_patch_landed(7, response) is False
    response.raise_for_status.assert_not_called()
