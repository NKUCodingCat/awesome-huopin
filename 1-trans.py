# -*- coding: utf-8 -*-

# FROM https://raw.githubusercontent.com/chinese-poetry/chinese-poetry-zhCN/master/trans.py
import six, os, re
from opencc import OpenCC


def convert_for_mw_t2s(words):
    cc = OpenCC('t2s')
    return cc.convert(words)


if six.PY2:
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')

POETRY_DIRECTORY = './json/'
TARGET_DIRECTORY = './json-zhcn'

def trans(name):
    file_path = os.path.join(POETRY_DIRECTORY, name)

    raw = open(file_path, 'r').read()

    if six.PY2:
        content = convert_for_mw_t2s(unicode(raw))
    else:
        content = convert_for_mw_t2s(raw)

    output_path = os.path.join(TARGET_DIRECTORY, name)

    with open(output_path, 'w') as f:
        f.write(content)
    
    


list(map(trans, os.listdir(POETRY_DIRECTORY)))