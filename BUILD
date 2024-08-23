load(':pytype.bzl', 'pytype_library')

pytype_library(name='deadline',
               srcs=['deadline.py'])

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
                     ':deadline',
                     ':response'])

py_test(name='filter_test',
        srcs=['filter_test.py'],
        deps=[':filter',
              ':blob',
              ':deadline',
              ':fake_endpoints'])

pytype_library(name='filter_adapters',
               srcs=['filter_adapters.py'],
               deps=[':filter'])

py_test(name='filter_adapters_test',
        srcs=['filter_adapters_test.py'],
        deps=[':filter_adapters',
              ':fake_endpoints',
              ':response'])

pytype_library(name='rest_endpoint',
               srcs=['rest_endpoint.py'],
               deps=[':blob',
                     ':deadline',
                     ':filter',
                     ':response'])

py_test(name='rest_endpoint_test',
        srcs=['rest_endpoint_test.py'],
        deps=[':rest_endpoint'])

pytype_library(name='rest_service_handler',
               srcs=['rest_service_handler.py'])

pytype_library(name='storage_schema',
               srcs=['storage_schema.py'],
               deps=[':rest_schema'])

pytype_library(name='version_cache',
               srcs=['version_cache.py'],
               deps=[':storage_schema'])

py_test(name='version_cache_test',
        srcs=['version_cache_test.py'],
        deps=[':version_cache',
              ':executor'])
        
pytype_library(name='storage',
               srcs=['storage.py'],
               data=['init_storage.sql',
                     'init_storage_postgres.sql'],
               deps=[':storage_schema',
                     ':rest_schema',
                     ':version_cache',
                     ':blob',
                     ':response',
                     ':filter'])

py_test(name='storage_test_sqlite',
        args=['StorageTestSqlite'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        deps=[':storage',
              ':rest_schema',
              ':version_cache'])

py_test(name='storage_test_sqlite_inmemory',
        args=['StorageTestSqliteInMemory'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        deps=[':storage',
              ':rest_schema',
              ':version_cache'])

py_test(name='storage_test_postgres',
        args=['StorageTestPostgres'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        deps=[':storage',
              ':rest_schema',
              ':version_cache'])

pytype_library(name='fake_endpoints',
               srcs=['fake_endpoints.py'],
               deps=['response',
                     'filter'])

pytype_library(name='dsn',
               srcs=['dsn.py'],
               deps=[':response'])

py_test(name='dsn_test',
        srcs=['dsn_test.py'],
        deps=[':dsn',
              ':response'])

pytype_library(name='output_handler',
               srcs=['output_handler.py'],
               deps=[':rest_service_handler',
                     ':filter',
                     ':response',
                     ':storage',
                     ':storage_schema',
                     ':message_builder',
                     ':dsn',
                     ':rest_schema'])

py_test(name='output_handler_test',
        srcs=['output_handler_test.py'],
        deps=[':output_handler',
              ':executor',
              ':fake_endpoints',
              ':response',
              ':storage',
              ':storage_schema'])

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
                     ':rest_schema',
                     ':storage'])

py_test(name='message_builder_filter_test',
        srcs=['message_builder_filter_test.py'],
        deps=[':blob',
              ':fake_endpoints',
              ':filter',
              ':message_builder_filter',
              ':response',
              ':rest_schema',
              ':storage'])

pytype_library(name='message_parser',
               srcs=['message_parser.py'],
               deps=[':blob'])

py_test(name='message_parser_test',
        srcs=['message_parser_test.py'],
        deps=[':message_parser',
              ':blob'],
        data=['testdata/multipart.msg',
              'testdata/multipart.json',
              'testdata/multipart2.msg',
              'testdata/multipart2.json',
              'testdata/dsn.msg',
              'testdata/dsn-text-rfc822-headers.msg',
              'testdata/related.msg',
              'testdata/related.json',])

pytype_library(name='message_parser_filter',
               srcs=['message_parser_filter.py'],
               deps=[':message_parser',
                     ':blob',
                     ':filter'],
        data=['testdata/multipart.msg'])

py_test(name='message_parser_filter_test',
        srcs=['message_parser_filter_test.py'],
        deps=[':message_parser_filter',
              ':blob',
              ':fake_endpoints',
              ':filter',
              ':response'])

pytype_library(name='address',
               srcs=['address.py'])

pytype_library(name='recipient_router_filter',
               srcs=['recipient_router_filter.py'],
               deps=[':filter',
                     ':response',
                     ':blob'])

py_test(name='recipient_router_filter_test',
        srcs=['recipient_router_filter_test.py'],
        deps=[':recipient_router_filter',
              ':fake_endpoints'])

pytype_library(name='address_list_policy',
               srcs=['address_list_policy.py'],
               deps=[':recipient_router_filter',
                     ':address',
                     ':response'])

py_test(name='address_list_policy_test',
        srcs=['address_list_policy_test.py'],
        deps=[':address_list_policy'])

pytype_library(name='dest_domain_policy',
               srcs=['dest_domain_policy.py'],
               deps=[':recipient_router_filter',
                     ':address',
                     ':response',
                     ':filter'])

pytype_library(name='dkim_endpoint',
               srcs=['dkim_endpoint.py'])

py_test(name='dkim_endpoint_test',
        srcs=['dkim_endpoint_test.py'],
        deps=[':dkim_endpoint',
              ':dest_domain_policy',
              ':fake_endpoints'])

pytype_library(name='dns_wrapper',
               srcs=['dns_wrapper.py'])

pytype_library(name='fake_dns_wrapper',
               srcs=['fake_dns_wrapper.py'],
               deps=[':dns_wrapper'])

pytype_library(name='mx_resolution',
               srcs=['mx_resolution.py'],
               deps=[':filter',
                     ':dns_wrapper'])

py_test(name='mx_resolution_test',
        srcs=['mx_resolution_test.py'],
        deps=[':mx_resolution',
              ':filter',
              ':fake_endpoints',
              ':fake_dns_wrapper'])

pytype_library(name='storage_writer_filter',
               srcs=['storage_writer_filter.py'],
               deps=[':blob',
                     ':deadline',
                     ':message_builder',
                     ':storage',
                     ':storage_schema',
                     ':filter',
                     ':response',
                     ':rest_schema'])

py_test(name='storage_writer_filter_test',
        srcs=['storage_writer_filter_test.py'],
        deps=[':storage_writer_filter',
              ':blob',
              ':filter',
              ':fake_endpoints',
              ':response',
              ':rest_schema',
              ':storage'])

pytype_library(name='exploder',
               srcs=['exploder.py'],
               deps=[
                   ':blob',
                   ':deadline',
                   ':executor',
                   ':filter',
                   ':response',
               ])

py_test(name='exploder_test',
        srcs=['exploder_test.py'],
        deps=[':exploder',
              ':fake_endpoints',
              ':blob',
              ':filter',
              ':response',
              ':storage'])

pytype_library(name='remote_host_filter',
               srcs=['remote_host_filter.py'],
               deps=[':filter'])

py_test(name='remote_host_filter_test',
        srcs=['remote_host_filter_test.py'],
        deps=[':remote_host_filter',
              ':fake_endpoints'])

pytype_library(name='received_header_filter',
               srcs=['received_header_filter.py'],
               deps=[':filter'])

py_test(name='received_header_filter_test',
        srcs=['received_header_filter_test.py'],
        deps=[':received_header_filter',
              ':fake_endpoints'])

pytype_library(name='relay_auth_filter',
               srcs=['relay_auth_filter.py'],
               deps=[':filter'])

py_test(name='relay_auth_filter_test',
        srcs=['relay_auth_filter_test.py'],
        deps=[':relay_auth_filter',
              ':fake_endpoints'])

# clients should get at this via ABC since it depends on ~everything?
pytype_library(name='config',
               srcs=['config.py'],
               deps=[':storage',
                     ':filter',
                     ':filter_adapters',
                     ':rest_endpoint',
                     ':address_list_policy',
                     ':dest_domain_policy',
                     ':message_builder_filter',
                     ':message_parser_filter',
                     ':recipient_router_filter',
                     ':dkim_endpoint',
                     ':mx_resolution',
                     ':storage_writer_filter',
                     ':exploder',
                     ':remote_host_filter',
                     ':received_header_filter',
                     ':relay_auth_filter'])

pytype_library(name='executor',
               srcs=['executor.py'])
py_test(name='executor_test',
        srcs=['executor_test.py'],
        deps=[':executor'])

pytype_library(name='hypercorn_main',
               srcs=['hypercorn_main.py'])

pytype_library(name='rest_service',
               srcs=['rest_service.py',
                     'rest_service_handler'])

pytype_library(name='fastapi_service',
               srcs=['fastapi_service.py',
                     'rest_service_handler'])

pytype_library(name='rest_schema',
               srcs=['rest_schema.py'])
py_test(name='rest_schema_test',
        srcs=['rest_schema_test.py'],
        deps=[':rest_schema'])

pytype_library(name='router_service',
               srcs=['router_service.py'],
               deps=[':storage',
                     ':version_cache',
                     ':storage_schema',
                     ':output_handler',
                     ':response',
                     ':executor',
                     ':config',
                     ':blob',
                     ':rest_endpoint',
                     ':rest_service',
                     ':fastapi_service',
                     ':hypercorn_main',
                     ':rest_endpoint_adapter'])

py_test(name='router_service_test_flask',
        srcs=['router_service_test.py'],
        main='router_service_test.py',
        args=['RouterServiceTestFlask'],
        deps=[':router_service',
              ':rest_endpoint',
              ':response',
              ':blob',
              ':config',
              ':fake_endpoints',
              ':filter'],
        data=['testdata/multipart.msg'])

py_test(name='router_service_test_fastapi',
        srcs=['router_service_test.py'],
        main='router_service_test.py',
        args=['RouterServiceTestFastApi'],
        deps=[':router_service',
              ':rest_endpoint',
              ':response',
              ':blob',
              ':config',
              ':fake_endpoints',
              ':filter'],
        data=['testdata/multipart.msg'])

pytype_library(name='smtp_auth',
               srcs=['smtp_auth.py'])

pytype_library(name='smtp_service',
               srcs=['smtp_service.py'],
               deps=[':blob',
                     ':smtp_auth',
                     ':response',
                     ':filter',
                     ':executor'])

py_test(name='smtp_service_test',
        srcs=['smtp_service_test.py'],
        deps=[':smtp_service',
              ':filter',
              ':response',
              ':fake_endpoints'])

pytype_library(name='rest_endpoint_adapter',
               srcs=['rest_endpoint_adapter.py'],
               deps=[':blob',
                     ':deadline',
                     ':executor',
                     ':filter',
                     ':response',
                     ':rest_schema',
                     ':rest_service',
                     ':rest_service_handler',
                     ':version_cache'])

py_test(name='rest_endpoint_adapter_test',
        srcs=['rest_endpoint_adapter_test.py'],
        deps=[':rest_endpoint_adapter',
              ':executor',
              ':fake_endpoints',
              ':filter',
              ':response',
              ':storage'])

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
                     ':fastapi_service',
                     ':hypercorn_main',
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
