"""

@create on: 2020.03.30
"""
import os
import re
import token
import tokenize

__all__ = ['check_code']

from typing import Tuple, List


class FunctionSrcChecker(object):
    # 禁用的函数库
    __forbidden_lib__ = {
        'os', 'sys', 'multiprocessing', 'concurrent', 'threading', 'threadpool',
        'asyncio', 'gevent', 'subprocess', 'pymysql', 'pymongo', 'aiomysql',
        'logging', 'loguru', 'socket', 'requests', 'urllib', 'urllib3', 'matplotlib',
        'cv2', 'PIL', 'atexit', 'socketserver', 'smtplib', 'smtpd', 'email', 'ssl', 'select',
        'selectors', 'asyncore', 'asynchat', 'signal', 'mmp', 'wsgiref', 'http', 'ftplib',
        'poplib', 'imaplib', 'nntplib', 'telnetlib', 'PyQt5', 'tkinter', 'io', 'shutil', 'pickle',
        'dmb', 'sqlite3', 'tempfile', 'shelve', 'csv', 'configparser', 'netrc', 'xdrlib', 'plistlib',
        '_thread', 'mmap', 'audioop', 'aifc', 'sunau', 'wave', 'chunk', 'cmd', 'tkinter', 'turtle',
        'xmlrpc', 'webbrowser', 'msilib', 'msvct', 'winreg', 'winsound', 'tty', 'posix', 'zlib',
        'gzip', 'bz2', 'lzma', 'zipfile', 'tarfile'
    }
    __forbidden_l_p__ = '(%s)'.join(__forbidden_lib__)
    __forbidden_l_pl__ = r'(from|import) +%s +(import)?' % __forbidden_l_p__

    # 禁用的方法
    __forbidden_function__ = (
        'open', 'exit', 'sleep', 'np.save', 'np.load', 'numpy.save', 'numpy.load',
        'pandas.read_sql', 'pd.read_sql', 'pandas.read_sql_query', 'pd.read_sql_query',
        'pandas.read_csv', 'pd.read_csv', 'pandas.read_sql_table', 'pd.read_sql_table'
    )
    __forbidden_f_p__ = '(%s)'.join(__forbidden_function__)
    __forbidden_f_pl__ = r'(with )?(.*.)?%s\(.*\)' % __forbidden_f_p__

    def check(self, module_path):
        if not os.path.exists(module_path):
            raise Exception('无法查找到算法路径，请确认输入的路径是否正确.')
        if os.path.isdir(module_path):
            has_init_ = False
            # print('输入的路径为文件夹。')
            for path in os.listdir(module_path):
                if path in {'__pycache__', '.idea'}:
                    continue
                if path.strip('__init__'):
                    has_init_ = True
                    # print('算法文件夹中拥有__init__.py文件。')
                self.check_code(f'{module_path}/{path}')
            if not has_init_:
                raise Exception('算法文件夹（包）中必须拥有__init__.py.')
        else:
            self.check_code(module_path)
        return True

    def format_code(self, fp) -> List[Tuple[int, str]]:
        """
        格式化代码, 去除注释

        :param fp: 文件

        :return: [[行数, 源代码], [行数, 源代码], ...]
        """
        prev_toktype = token.INDENT
        last_lineno = 1
        last_col = 0
        line = ''
        source = list()

        for toktype, ttext, (slineno, scol), (elineno, ecol), ltext in tokenize.generate_tokens(fp.readline):
            # print(toktype, prev_toktype)
            if toktype == token.NL:
                continue
            if elineno != last_lineno:
                source.append((last_lineno, line.strip('\n\r\t ')))
                line = ''
            if slineno > last_lineno:
                last_col = 0
            if scol > last_col:
                line += " " * (scol - last_col)
            if (toktype == token.STRING and prev_toktype in {token.INDENT, token.COMMENT}) or toktype == token.COMMENT:
                pass
            else:
                line += ttext
            prev_toktype = toktype
            last_col = ecol
            last_lineno = elineno
        return source

    def check_code(self, path):
        with open(path, 'r', encoding='utf-8') as file:
            for lindex, line in self.format_code(file):
                lib = self.check_lib(line)
                if lib is not None:
                    raise Exception(f'{path}文件于第 {lindex} 行查找到禁止使用的函数库: {lib}, 请修改代码.')
                fun = self.check_function(line)
                if fun is not None:
                    raise Exception(f'{path}文件于第 {lindex} 行查找到禁止使用的接口: {fun}, 请修改代码.')

    def _match(self, pl, p, line):
        if re.match(pl, line):
            return re.findall(p, line)[0]
        return None

    def check_lib(self, line):
        # 禁用库，通过import检查
        return self._match(self.__forbidden_l_pl__, self.__forbidden_l_p__, line)

    def check_function(self, line):
        # 禁用方法
        return self._match(self.__forbidden_f_pl__, self.__forbidden_f_p__, line)


check_code = FunctionSrcChecker().check
