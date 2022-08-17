import subprocess
import sys
from time import sleep
from typing import List, Callable


def call_curl(
        address: str,
        timeout=None,
) -> str:
    """Simple rapper around subprocess.
    Will throw an error if thing goes wrong.
    """
    process = subprocess.run(
        ['curl', address],
        capture_output=True,
        timeout=timeout,
        check=True,
        text=True,
    )
    return process.stdout


def call_netcat(
        address: str,
        port: int,
        data: str,
        timeout=None,
) -> str:
    """Simple rapper around subprocess.
    Will throw an error if thing goes wrong.
    """
    process = subprocess.run(
        ['nc', address, str(port)],
        capture_output=True,
        timeout=timeout,
        check=True,
        text=True,
        input=data,
    )
    return process.stdout


def call_until_true(
        name: str,
        sleep_time_sec: int,
        max_calls: int,
        func: Callable[[], bool],
) -> bool:
    for i in range(max_calls):
        print(f'Calling {name} (repeat {i})')
        try:
            if func():
                return True
        except Exception as e:
            print(f'Encountered exception: {e}')
        print(f'Sleeping for {sleep_time_sec} seconds')
        sleep(sleep_time_sec)
    return False


def exit_wait_for_healthy(
        service_name: str,
        healthy: bool,
):
    if healthy:
        print(f'{service_name} healthy')
        sys.exit()
    else:
        sys.exit(f'{service_name} took too long to become healthy')
