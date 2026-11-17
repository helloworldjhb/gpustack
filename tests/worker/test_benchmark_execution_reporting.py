import asyncio
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

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


@pytest.mark.parametrize(
    'body',
    [
        {},
        {'details': {'code': 'benchmark_target_changed'}},
        {'details': {'code': 'unrelated_conflict'}},
        {'details': None},
    ],
)
def test_conflicts_that_do_not_retire_an_execution_still_raise(body):
    response = httpx.Response(
        409,
        json=body,
        request=httpx.Request('PATCH', 'http://server/benchmarks/7/state'),
    )
    with pytest.raises(httpx.HTTPStatusError):
        manager.BenchmarkManager._state_patch_landed(7, response)


@pytest.mark.asyncio
async def test_retired_enqueue_is_skipped_and_the_next_execution_can_start():
    patches = []

    def patch(request):
        patches.append(request)
        if request.url.path == '/benchmarks/7/state':
            return httpx.Response(
                409, json={'details': {'code': 'benchmark_run_changed'}}
            )
        return httpx.Response(200)

    async with httpx.AsyncClient(
        base_url='http://server', transport=httpx.MockTransport(patch)
    ) as client:
        instance = object.__new__(manager.BenchmarkManager)
        instance._clientset_getter = lambda: SimpleNamespace(
            http_client=SimpleNamespace(get_async_httpx_client=lambda: client)
        )
        instance._benchmark_queue = deque()
        instance._queue_lock = asyncio.Lock()
        instance._canceled_ids = set()
        instance._torn_down_ids = {}
        instance._active_benchmark_id = None
        started = []

        async def start(benchmark):
            started.append(benchmark.id)
            raise asyncio.CancelledError

        instance._start_benchmark = start
        for id in (7, 8):
            await instance._enqueue_benchmark(
                SimpleNamespace(id=id, name=f'bench-{id}', run_id=f'run-{id}')
            )
        with pytest.raises(asyncio.CancelledError):
            await instance._benchmark_queue_worker()

    assert started == [8]
    assert not instance._benchmark_queue
    assert len(patches) == 2
    assert [request.url.path for request in patches] == [
        '/benchmarks/7/state',
        '/benchmarks/8/state',
    ]
