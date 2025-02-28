"""

@create on: 2020.09.24
删除动态链接库
"""
import os
import pip

package_lib = 'scdap'
site_packages_dir = os.path.dirname(os.path.dirname(pip.__file__))

link_path = os.path.join(site_packages_dir, package_lib)

if os.path.exists(link_path):
    stdin = input(f'{link_path}动态链接已经存在, 是否删除(y/n, default=y)?')
    if stdin == '' or stdin == 'y':
        os.remove(link_path)
        print(f'已经删除动态链接{link_path}.')
    else:
        exit(0)
