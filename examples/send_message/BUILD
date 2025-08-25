exports_files(['body.txt'])

py_library(name='trivial_json_sender',
           srcs=['trivial_json_sender.py'],
           visibility=['//visibility:public'])

py_library(name='trivial_rfc822_sender',
           srcs=['trivial_rfc822_sender.py'],
           visibility=['//visibility:public'])

py_library(name='send_message',
           srcs=['send_message.py'],
           deps=['@pypi//requests'],
           visibility=['//visibility:public'])

test_suite(name='all_tests')
