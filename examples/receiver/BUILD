py_library(name='receiver',
           srcs=['receiver.py'],
           tags=['no-mypy'],
           visibility=['//visibility:public'])

py_library(name='fastapi_receiver',
           srcs=['fastapi_receiver.py'],
           deps=[':receiver',
                 '@pypi//fastapi'],
           visibility=['//visibility:public'])

py_library(name='flask_receiver',
           srcs=['flask_receiver.py'],
           deps=[':receiver',
                 '@pypi//flask'],
           visibility=['//visibility:public'])

test_suite(
    name='all_tests',
    tags=['-broken']
)
