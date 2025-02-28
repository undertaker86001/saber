"""

@create on: 2021.04.19
"""


class HealthDefineItem(object):
    health_name: str
    reverse: int
    limit: bool


class StatusDefineItem(object):
    status_code: int
    status_name: str
    cn_name: str
    en_name: str

    description: str = ''
    enabled: int = 1
