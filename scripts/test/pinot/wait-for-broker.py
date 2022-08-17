from helpers import (call_curl, call_until_true, exit_wait_for_healthy)

bash_timout_sec = None
sleep_time_sec = 5
max_calls = 12
service_name = 'Pinot Broker'


def func():
    output: str = call_curl(
        address='localhost:8099/health',
        timeout=bash_timout_sec
    ).strip()
    print(f'Response from broker: {output}')
    return 'OK' in output


healthy = call_until_true(
    name=service_name,
    sleep_time_sec=sleep_time_sec,
    max_calls=max_calls,
    func=func,
)

exit_wait_for_healthy(
    service_name=service_name,
    healthy=healthy,
)
