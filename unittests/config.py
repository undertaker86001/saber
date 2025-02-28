"""

@create on: 2021.01.26
"""

# rabbitmq_config = {
#         'host': '119.3.26.214',
#         'port': 5672,
#         'user': 'dap-cicd',
#         'password': 'O2rHMeiX',
#         'vhost': '/dap-cicd',
#         'new_mqbase': True
#     }


rabbitmq_config = {
        'host': '172.25.100.9',
        'port': 5672,
        'user': 'sucheon-dap',
        'password': 'sucheon-dap',
        'vhost': '/',
        'new_mqbase': True
    }


mysql_config = {
    'host': '172.25.100.9',
    'port': 3306,
    'user': 'sucheon',
    'password': 'sucheon',
    'db': 'sucheon_server_scdap_test'
}
