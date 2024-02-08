# TODO it seems like there isn't a standard recipe to run static
# analysis tools e.g. pytype/pylint from bazel though it shouldn't be
# too hard to write some wrappers for py_library/py_test to do it:
# https://stackoverflow.com/questions/70962005/can-i-add-static-analysis-to-a-py-binary-or-py-library-rule
# possibly via validation actions
# https://bazel.build/extending/rules#validation-actions

py_library(name='blob',
           srcs=['blob.py'])

py_test(name='blob_test',
        srcs=['blob_test.py'],
        deps=[':blob'])

py_library(name='response',
           srcs=['response.py'])
py_library(name='filter',
           srcs=['filter.py'],
           deps=[':blob',
                 ':response'])
py_test(name='filter_test',
        srcs=['filter_test.py'],
        deps=[':filter'])


py_library(name='rest_endpoint',
           srcs=['rest_endpoint.py'],
           deps=['filter',
                 'response',
                 'blob'])

py_test(name='rest_endpoint_test',
        srcs=['rest_endpoint_test.py'],
        deps=[':rest_endpoint'])

py_library(name='rest_service_handler',
           srcs=['rest_service_handler.py'])

py_library(name='storage_schema',
           srcs=['storage_schema.py'])

py_library(name='storage',
           srcs=['storage.py'],
           data=['init_storage.sql'],
           deps=[':storage_schema',
                 ':blob',
                 ':response',
                 ':filter'])

py_test(name='storage_test',
        srcs=['storage_test.py'],
        data=['storage_test_recovery.sql'],
        deps=[':storage'])

py_library(name='transaction',
           srcs=['transaction.py'],
           deps=['rest_service_handler',
                 'filter',
                 'response',
                 'storage',
                 'storage_schema'])

py_library(name='fake_endpoints',
           srcs=['fake_endpoints.py'],
           deps=['response',
                 'filter'])

py_test(name='transaction_test',
        srcs=['transaction_test.py'],
        deps=[':transaction',
              ':fake_endpoints'])

py_library(name='address',
           srcs=['address.py'])

py_library(name='recipient_router_filter',
           srcs=['recipient_router_filter.py'],
           deps=[':filter',
                 ':response',
                 ':blob'])

py_library(name='local_domain_policy',
           srcs=['local_domain_policy.py'],
           deps=[':recipient_router_filter',
                 ':address',
                 ':response'])
py_library(name='dest_domain_policy',
           srcs=['dest_domain_policy.py'],
           deps=[':recipient_router_filter',
                 ':address',
                 ':response',
                 ':filter'])

py_test(name='recipient_router_filter_test',
        srcs=['recipient_router_filter_test.py'],
        deps=[':recipient_router_filter',
              ':dest_domain_policy',
              ':fake_endpoints'])

py_library(name='dkim_endpoint',
           srcs=['dkim_endpoint.py'])
py_test(name='dkim_endpoint_test',
        srcs=['dkim_endpoint_test.py'],
        deps=[':dkim_endpoint',
              ':dest_domain_policy',
              ':fake_endpoints'])


py_library(name='mx_resolution',
           srcs=['mx_resolution.py'])
py_library(name='storage_writer_filter',
           srcs=['storage_writer_filter.py'])

py_test(name='storage_writer_filter_test',
        srcs=['storage_writer_filter_test.py'],
        deps=[':storage_writer_filter',
              'blob',
              'filter',
              'fake_endpoints',
              'response',
              'storage'])

py_library(name='exploder',
           srcs=['exploder.py'])


py_test(name='exploder_test',
        srcs=['exploder_test.py'],
        deps=[':exploder',
              ':fake_endpoints',
              ':blob',
              ':filter',
              ':response',
              ':storage'])

py_library(name='config',
           srcs=['config.py'],
           deps=[':storage',
                 ':filter',
                 ':rest_endpoint',
                 ':local_domain_policy',
                 ':dest_domain_policy',
                 ':recipient_router_filter',
                 ':dkim_endpoint',
                 ':mx_resolution',
                 ':storage_writer_filter',
                 ':exploder'])
py_library(name='tags',
           srcs=['tags.py'])
py_library(name='executor',
           srcs=['executor.py'],
           deps=['tags'])


py_library(name='blobs',
           srcs=['blobs.py'])

py_library(name='gunicorn_main',
           srcs=['gunicorn_main.py'])

py_library(name='rest_service',
           srcs=['rest_service.py',
                 'rest_service_handler',
                 'tags',
                 'blobs',
                 'blob',
                 'response'])

py_library(name='router_service',
           srcs=['router_service.py'],
           deps=[':storage',
                 ':transaction',
                 ':response',
                 ':tags',
                 ':executor',
                 ':config',
                 ':blob',
                 ':rest_endpoint',
                 ':blobs',
                 ':rest_service',
                 ':gunicorn_main'])

py_test(name='router_service_test',
        srcs=['router_service_test.py'],
        deps=[':router_service',
              ':rest_endpoint',
              ':response',
              ':blob',
              ':config',
              ':fake_endpoints',
              ':filter'])

py_library(name='smtp_auth',
           srcs=['smtp_auth.py'])

py_library(name='smtp_service',
           srcs=['smtp_service.py'],
           deps=[':blob',
                 ':smtp_auth',
                 ':response',
                 ':filter'])

py_test(name='smtp_service_test',
        srcs=['smtp_service_test.py'],
        deps=[':smtp_service',
              ':filter',
              ':response',
              ':fake_endpoints'])

py_library(name='rest_endpoint_adapter',
           srcs=['rest_endpoint_adapter.py'],
           deps=[':rest_service_handler'])

py_library(name='smtp_endpoint',
           srcs=['smtp_endpoint.py'],
           deps=[])

py_library(name='gateway',
           srcs=['gateway.py'],
           deps=[':rest_endpoint_adapter',
                 ':smtp_endpoint',
                 ':rest_endpoint',
                 ':smtp_service',
                 ':rest_service',
                 ':gunicorn_main',
                 ':config'])

py_library(name='fake_smtpd',
           srcs=['fake_smtpd.py'])

py_test(name='gateway_test',
        srcs=['gateway_test.py'],
        deps=[':gateway',
              ':rest_endpoint',
              ':fake_smtpd',
              ':blob',
              ':config'])

test_suite(
    name='all_tests',
    tags=['-broken']
)
