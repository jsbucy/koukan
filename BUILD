load(':pytype.bzl', 'pytype_library')

pytype_library(name='blob',
               srcs=['blob.py'])

py_test(name='blob_test',
        srcs=['blob_test.py'],
        deps=[':blob'])

pytype_library(name='response',
               srcs=['response.py'])
pytype_library(name='filter',
               srcs=['filter.py'],
               deps=[':blob',
                     ':response'])
py_test(name='filter_test',
        srcs=['filter_test.py'],
        deps=[':filter'])


pytype_library(name='rest_endpoint',
               srcs=['rest_endpoint.py'],
               deps=['filter',
                     'response',
                     'blob'])

py_test(name='rest_endpoint_test',
        srcs=['rest_endpoint_test.py'],
        deps=[':rest_endpoint'])

pytype_library(name='rest_service_handler',
               srcs=['rest_service_handler.py'])

pytype_library(name='storage_schema',
               srcs=['storage_schema.py'])

pytype_library(name='storage',
               srcs=['storage.py'],
               data=['init_storage.sql',
                     'init_storage_postgres.sql'],
               deps=[':storage_schema',
                     ':blob',
                     ':response',
                     ':filter'])

py_test(name='storage_test_sqlite',
        args=['StorageTestSqlite'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        data=['storage_test_recovery.sql'],
        deps=[':storage'])

py_test(name='storage_test_postgres',
        args=['StorageTestPostgres'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        data=['storage_test_recovery.sql'],
        deps=[':storage'])

pytype_library(name='fake_endpoints',
               srcs=['fake_endpoints.py'],
               deps=['response',
                     'filter'])

pytype_library(name='transaction',
               srcs=['transaction.py'],
               deps=['rest_service_handler',
                     'filter',
                     ':message_builder',
                     'response',
                     'storage',
                     'storage_schema'])

py_test(name='transaction_test',
        srcs=['transaction_test.py'],
        deps=[':transaction',
              ':fake_endpoints'])

pytype_library(name='output_handler',
               srcs=['output_handler.py'],
               deps=['rest_service_handler',
                     'filter',
                     'response',
                     'storage',
                     'storage_schema',
                     'message_builder'])

py_test(name='output_handler_test',
        srcs=['output_handler_test.py'],
        deps=[':output_handler',
              ':executor',
              ':fake_endpoints',
              ':transaction'])


pytype_library(name='message_builder',
               srcs=['message_builder.py'],
               deps=[':blob'])

py_test(name='message_builder_test',
        srcs=['message_builder_test.py'],
        deps=[':message_builder'],
        data=['message_builder.json'])

pytype_library(name='message_builder_filter',
               srcs=['message_builder_filter.py'],
               deps=[':blob',
                     ':filter',
                     ':message_builder',
                     ':storage'])

py_test(name='message_builder_filter_test',
        srcs=['message_builder_filter_test.py'],
        deps=[':blob',
              ':fake_endpoints',
              ':filter',
              ':message_builder_filter',
              ':response',
              ':storage'])


pytype_library(name='address',
               srcs=['address.py'])

pytype_library(name='recipient_router_filter',
               srcs=['recipient_router_filter.py'],
               deps=[':filter',
                     ':response',
                     ':blob'])

pytype_library(name='local_domain_policy',
               srcs=['local_domain_policy.py'],
               deps=[':recipient_router_filter',
                     ':address',
                     ':response'])
pytype_library(name='dest_domain_policy',
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

pytype_library(name='dkim_endpoint',
               srcs=['dkim_endpoint.py'])
py_test(name='dkim_endpoint_test',
        srcs=['dkim_endpoint_test.py'],
        deps=[':dkim_endpoint',
              ':dest_domain_policy',
              ':fake_endpoints'])


pytype_library(name='mx_resolution',
               srcs=['mx_resolution.py'])
pytype_library(name='storage_writer_filter',
               srcs=['storage_writer_filter.py'])

py_test(name='storage_writer_filter_test',
        srcs=['storage_writer_filter_test.py'],
        deps=[':storage_writer_filter',
              'blob',
              'filter',
              'fake_endpoints',
              'response',
              'storage'])

pytype_library(name='exploder',
               srcs=['exploder.py'])


py_test(name='exploder_test',
        srcs=['exploder_test.py'],
        deps=[':exploder',
              ':fake_endpoints',
              ':blob',
              ':filter',
              ':response',
              ':storage'])

# clients should get at this via ABC since this depends on ~everything?
pytype_library(name='config',
               srcs=['config.py'],
               deps=[':storage',
                     ':filter',
                     ':rest_endpoint',
                     ':local_domain_policy',
                     ':dest_domain_policy',
                     ':message_builder_filter',
                     ':recipient_router_filter',
                     ':dkim_endpoint',
                     ':mx_resolution',
                     ':storage_writer_filter',
                     ':exploder'])

pytype_library(name='executor',
               srcs=['executor.py'])


pytype_library(name='blobs',
               srcs=['blobs.py'])

pytype_library(name='gunicorn_main',
               srcs=['gunicorn_main.py'])

pytype_library(name='rest_service',
               srcs=['rest_service.py',
                     'rest_service_handler',
                     'blobs',
                     'blob',
                     'response'])

pytype_library(name='router_service',
               srcs=['router_service.py'],
               deps=[':storage',
                     ':transaction',
                     ':output_handler',
                     ':response',
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

pytype_library(name='smtp_auth',
               srcs=['smtp_auth.py'])

pytype_library(name='smtp_service',
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

pytype_library(name='rest_endpoint_adapter',
               srcs=['rest_endpoint_adapter.py'],
               deps=[':rest_service_handler'])

pytype_library(name='smtp_endpoint',
               srcs=['smtp_endpoint.py'],
               deps=[])

pytype_library(name='gateway',
               srcs=['gateway.py'],
               deps=[':rest_endpoint_adapter',
                     ':smtp_endpoint',
                     ':rest_endpoint',
                     ':smtp_service',
                     ':rest_service',
                     ':gunicorn_main',
                     ':config'])

pytype_library(name='fake_smtpd',
               srcs=['fake_smtpd.py'])

py_test(name='gateway_test',
        srcs=['gateway_test.py'],
        deps=[':gateway',
              ':rest_endpoint',
              ':fake_smtpd',
              ':blob',
              ':config'])

test_suite(
    name='pytype_tests',
    tags=['pytype_test']
)

test_suite(
    name='all_tests',
    tags=['-broken']
)
