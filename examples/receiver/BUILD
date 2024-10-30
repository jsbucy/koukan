load('//:pytype.bzl', 'pytype_library')

pytype_library(name='receiver',
               srcs=['receiver.py'],
               visibility=['//visibility:public'])

test_suite(
    name='all_tests',
    tags=['-broken']
)
