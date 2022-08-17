from helpers import (call_netcat, call_until_true, exit_wait_for_healthy)

bash_timout_sec = None
sleep_time_sec = 1
max_calls = 30
service_name = 'Zookeeper'


def func():
    output: str = call_netcat(
        address='localhost',
        port=2181,
        data='srvr',
        timeout=bash_timout_sec
    )
    print(f'Response from broker: {output}')
    return 'Mode: standalone' in output


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
