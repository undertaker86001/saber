"""

@create on: 2020.09.25
python3.7 -c start -n xxx -d
"""


def main(*args, **kwargs):
    import warnings
    warnings.filterwarnings('ignore')
    from .execute import execute_process
    execute_process(*args, **kwargs)


if __name__ == '__main__':
    main()
