import requests, json

from helpers.data_structures import Setting


def patch_scheduler_config():
    settings = [Setting("scheduling", False), Setting("scheduler_iteration", 200), Setting("node_check_rate", 100), Setting("tcp_timeout", 10), Setting("keep_completed", 3600)]

    payload = json.dumps(settings, default=lambda o: o._try(o))
    params = {"token": "Zrt5q2gWKZ4JpuLyoJf5gH5zjoKMujfYWz2hhEMaxutDmEfe-Fo5Atvz4lHoPIty_c9j8e5xguCi2JVKk3fyBQ"}
    
    res = requests.patch("http://127.0.0.1:5000/scheduler/server", params=params, json=payload)
    print(res.json())
    