"""

@create on: 2020.09.24
将算法库动态链接至site-packages中
通过动态链接的方式, 将运行外部程序直接
import scdap_function
"""
import os
import pip
import subprocess


package_lib = 'scdap'
site_packages_dir = os.path.dirname(os.path.dirname(pip.__file__))

if not os.path.exists(package_lib):
    os.chdir('../')
    if not os.path.exists(package_lib):
        print('请确保算法库目录路径正确存放.')

package_lib_path = os.path.abspath(package_lib)
link_path = os.path.join(site_packages_dir, package_lib)
print(package_lib_path, '->', link_path)

if os.path.exists(link_path):
    stdin = input(f'{link_path}动态链接已经存在, 是否删除(y/n, default=y)?')
    if stdin == '' or stdin == 'y':
        os.remove(link_path)
        print(f'已经删除旧的动态链接{link_path}.')
    else:
        exit(0)


if os.name == 'posix':
    output = subprocess.getoutput(f'sudo ln -s {package_lib_path} {link_path}')
    print(output)
elif os.name == 'nt':
    output = subprocess.getoutput(f'mklink /d {link_path} {package_lib_path}')
    print(output)
else:
    print('非法的操作系统类型, 只允许使用window/linux.')
    exit(1)
