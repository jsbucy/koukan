load("@rules_python//python:py_library.bzl", "py_library")

load('//:pytype.bzl', 'pytype_library')

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
                     ':message_builder',
                     ':response',
                     ':rest_schema'])

py_test(name='filter_test',
        srcs=['filter_test.py'],
        deps=[':filter',
              ':blob',
              ':deadline',
              ':fake_endpoints'])

pytype_library(name='async_filter_wrapper',
               srcs=['async_filter_wrapper.py'],
               deps=[':backoff',
                     ':blob',
                     ':deadline',
                     ':response',
                     ':storage_schema'])

py_test(name='async_filter_wrapper_test',
        srcs=['async_filter_wrapper_test.py'],
        deps=[':async_filter_wrapper',
              ':blob',
              ':deadline',
              ':fake_endpoints',
              ':filter',
              ':response',
              ':storage_schema'])

pytype_library(name='rest_endpoint',
               srcs=['rest_endpoint.py'],
               deps=[':blob',
                     ':deadline',
                     ':filter',
                     ':response',
                     '@pypi//h2',
                     '@pypi//httpx',
                     '@pypi//werkzeug'])

py_test(name='rest_endpoint_test',
        srcs=['rest_endpoint_test.py'],
        deps=[':rest_endpoint',
              '@pypi//werkzeug'])

pytype_library(name='rest_service_handler',
               srcs=['rest_service_handler.py'],
               deps=['@pypi//fastapi',
                     '@pypi//flask'])

pytype_library(name='storage_schema',
               srcs=['storage_schema.py'],
               deps=[':rest_schema',
                     ':blob'])

pytype_library(name='backoff',
               srcs=['backoff.py'])

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
               deps=[':backoff',
                     ':storage_schema',
                     ':rest_schema',
                     ':version_cache',
                     ':blob',
                     ':response',
                     ':filter',
                     '@pypi//sqlalchemy'])

py_test(name='storage_test_sqlite',
        args=['StorageTestSqlite'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        deps=[':storage',
              ':rest_schema',
              ':version_cache',
              ':postgres_test_utils',
              ':sqlite_test_utils',
              '@pypi//sqlalchemy'])

py_test(name='storage_test_postgres',
        args=['StorageTestPostgres'],
        main='storage_test.py',
        srcs=['storage_test.py'],
        deps=[':storage',
              ':rest_schema',
              ':version_cache',
              ':postgres_test_utils',
              ':sqlite_test_utils'])

pytype_library(name='fake_endpoints',
               srcs=['fake_endpoints.py'],
               deps=[':response',
                     ':filter',
                     ':storage_schema'])

pytype_library(name='dsn',
               srcs=['dsn.py'],
               deps=[':response'])

py_test(name='dsn_test',
        srcs=['dsn_test.py'],
        deps=[':dsn',
              ':response'])

pytype_library(name='output_handler',
               srcs=['output_handler.py'],
               deps=[':backoff',
                     ':rest_service_handler',
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
              ':sqlite_test_utils',
              ':storage',
              ':storage_schema'])

pytype_library(name='message_builder',
               srcs=['message_builder.py'],
               deps=[':blob',
                     ':rest_schema',
                     ':storage_schema'])

py_test(name='message_builder_test',
        srcs=['message_builder_test.py'],
        deps=[':message_builder'],
        data=['//testdata:message_builder.json'])

pytype_library(name='message_builder_filter',
               srcs=['message_builder_filter.py'],
               deps=[':blob',
                     ':filter',
                     ':message_builder',
                     ':response',
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
              ':sqlite_test_utils',
              ':storage',
              ':storage_schema'])

pytype_library(name='message_parser',
               srcs=['message_parser.py'],
               deps=[':blob'])

py_test(name='message_parser_test',
        srcs=['message_parser_test.py'],
        deps=[':message_parser',
              ':blob'],
        data=['//testdata:multipart.msg',
              '//testdata:multipart.json',
              '//testdata:multipart2.msg',
              '//testdata:multipart2.json',
              '//testdata:dsn.msg',
              '//testdata:dsn-text-rfc822-headers.msg',
              '//testdata:related.msg',
              '//testdata:related.json',])

pytype_library(name='message_parser_filter',
               srcs=['message_parser_filter.py'],
               deps=[':message_parser',
                     ':blob',
                     ':filter'])

py_test(name='message_parser_filter_test',
        srcs=['message_parser_filter_test.py'],
        deps=[':message_parser_filter',
              ':blob',
              ':fake_endpoints',
              ':filter',
              ':response'],
        data=['//testdata:multipart.msg'])

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
               srcs=['dkim_endpoint.py'],
               deps=['@pypi//dkimpy'])

py_test(name='dkim_endpoint_test',
        srcs=['dkim_endpoint_test.py'],
        deps=[':dkim_endpoint',
              ':dest_domain_policy',
              ':fake_endpoints',
              '@pypi//dkimpy'])

pytype_library(name='dns_wrapper',
               srcs=['dns_wrapper.py'],
               deps=['@pypi//dnspython'])

pytype_library(name='fake_dns_wrapper',
               srcs=['fake_dns_wrapper.py'],
               deps=[':dns_wrapper',
                     '@pypi//dnspython'])

pytype_library(name='mx_resolution',
               srcs=['mx_resolution.py'],
               deps=[':filter',
                     ':dns_wrapper',
                     ':response',
                     '@pypi//dnspython'])

py_test(name='mx_resolution_test',
        srcs=['mx_resolution_test.py'],
        deps=[':mx_resolution',
              ':filter',
              ':fake_endpoints',
              ':fake_dns_wrapper',
              '@pypi//dnspython'])

pytype_library(name='storage_writer_filter',
               srcs=['storage_writer_filter.py'],
               deps=[':backoff',
                     ':blob',
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
              ':sqlite_test_utils',
              ':storage'])

pytype_library(name='exploder',
               srcs=['exploder.py'],
               deps=[
                   ':blob',
                   ':deadline',
                   ':filter',
                   ':response',
               ])

py_test(name='exploder_test',
        srcs=['exploder_test.py'],
        deps=[':exploder',
              ':async_filter_wrapper',
              ':blob',
              ':fake_endpoints',
              ':filter',
              ':postgres_test_utils',
              ':response',
              ':storage_writer_filter',
              ':storage'])

pytype_library(name='add_route_filter',
               srcs=['add_route_filter.py'],
               deps=[':filter'])

py_test(name='add_route_filter_test',
        srcs=['add_route_filter_test.py'],
        deps=[':add_route_filter',
              ':blob',
              ':fake_endpoints',
              ':filter',
              ':response'])

pytype_library(name='remote_host_filter',
               srcs=['remote_host_filter.py'],
               deps=[':filter',
                     ':dns_wrapper'])

py_test(name='remote_host_filter_test',
        srcs=['remote_host_filter_test.py'],
        deps=[':remote_host_filter',
              ':fake_endpoints',
              ':fake_dns_wrapper',
              '@pypi//dnspython'])

pytype_library(name='received_header_filter',
               srcs=['received_header_filter.py'],
               deps=[':filter'])

py_test(name='received_header_filter_test',
        srcs=['received_header_filter_test.py'],
        deps=[':received_header_filter',
              ':fake_endpoints'])

pytype_library(name='relay_auth_filter',
               srcs=['relay_auth_filter.py'],
               deps=[':filter',
                     ':response'])

py_test(name='relay_auth_filter_test',
        srcs=['relay_auth_filter_test.py'],
        deps=[':relay_auth_filter',
              ':fake_endpoints'])

pytype_library(name='filter_chain_factory',
               srcs=['filter_chain_factory.py'],
               deps=[':filter'])

pytype_library(name='recipient_router_factory',
               srcs=['recipient_router_factory.py'],
               deps=[':filter',
                     ':address_list_policy',
                     ':dest_domain_policy',
                     ':recipient_router_filter'])

pytype_library(name='filter_chain_wiring',
               srcs=['filter_chain_wiring.py'],
               deps=[':add_route_filter',
                     ':async_filter_wrapper',
                     ':dkim_endpoint',
                     ':exploder',
                     ':filter',
                     ':filter_chain_factory',
                     ':message_builder_filter',
                     ':message_parser_filter',
                     ':mx_resolution',
                     ':received_header_filter',
                     ':recipient_router_factory',
                     ':relay_auth_filter',
                     ':remote_host_filter',
                     ':rest_endpoint'])

pytype_library(name='executor',
               srcs=['executor.py'])
py_test(name='executor_test',
        srcs=['executor_test.py'],
        deps=[':executor'])

pytype_library(name='hypercorn_main',
               srcs=['hypercorn_main.py'],
               deps=['@pypi//hypercorn'])

pytype_library(name='fastapi_service',
               srcs=['fastapi_service.py'],
               deps=[':rest_service_handler',
                     '@pypi//fastapi'])

pytype_library(name='rest_schema',
               srcs=['rest_schema.py'])
py_test(name='rest_schema_test',
        srcs=['rest_schema_test.py'],
        deps=[':rest_schema'])

pytype_library(name='router_service',
               srcs=['router_service.py'],
               deps=[':blob',
                     ':executor',
                     ':fastapi_service',
                     ':filter',
                     ':filter_chain_factory',
                     ':filter_chain_wiring',
                     ':hypercorn_main',
                     ':output_handler',
                     ':rest_handler',
                     ':storage_writer_filter',
                     ':storage',
                     '@pypi//pyyaml'])

py_test(name='router_service_test_fastapi',
        srcs=['router_service_test.py'],
        main='router_service_test.py',
        args=['RouterServiceTestFastApi'],
        deps=[':router_service',
              ':async_filter_wrapper',
              ':rest_endpoint',
              ':response',
              ':blob',
              ':fake_endpoints',
              ':filter',
              ':postgres_test_utils',
              ':sqlite_test_utils',
              ':hello_filter'],
        data=['//testdata:multipart.msg'])

py_test(name='router_service_test_sqlite',
        srcs=['router_service_test.py'],
        main='router_service_test.py',
        args=['RouterServiceTestSqlite'],
        deps=[':router_service',
              ':async_filter_wrapper',
              ':rest_endpoint',
              ':response',
              ':blob',
              ':fake_endpoints',
              ':filter',
              ':postgres_test_utils',
              ':sqlite_test_utils',
              ':hello_filter'],
        data=['//testdata:multipart.msg'])

pytype_library(name='smtp_auth',
               srcs=['smtp_auth.py'],
               deps=['@pypi//aiosmtpd'])

pytype_library(name='smtp_service',
               srcs=['smtp_service.py'],
               deps=[':blob',
                     ':smtp_auth',
                     ':response',
                     ':filter',
                     ':executor',
                     '@pypi//aiosmtpd'])

py_test(name='smtp_service_test',
        srcs=['smtp_service_test.py'],
        deps=[':smtp_service',
              ':executor',
              ':filter',
              ':response',
              ':fake_endpoints'])

pytype_library(name='rest_handler',
               srcs=['rest_handler.py'],
               deps=[':blob',
                     ':deadline',
                     ':executor',
                     ':filter',
                     ':response',
                     ':rest_schema',
                     ':rest_service_handler',
                     ':version_cache',
                     '@pypi//fastapi',
                     '@pypi//flask',
                     '@pypi//werkzeug'])

py_test(name='rest_handler_test',
        srcs=['rest_handler_test.py'],
        deps=[':rest_handler',
              ':executor',
              ':fake_endpoints',
              ':filter',
              ':response',
              ':storage',
              '@pypi//fastapi',
              '@pypi//flask',
              '@pypi//werkzeug'])

pytype_library(name='sync_filter_adapter',
               srcs=['sync_filter_adapter.py'],
               deps=[':blob',
                     ':deadline',
                     ':executor',
                     ':filter',
                     ':response',
                     ':rest_schema',
                     ':rest_service_handler',
                     ':version_cache',
                     '@pypi//fastapi',
                     '@pypi//flask',
                     '@pypi//werkzeug'])

py_test(name='sync_filter_adapter_test',
        srcs=['sync_filter_adapter_test.py'],
        deps=[':sync_filter_adapter',
              ':executor',
              ':fake_endpoints',
              ':filter',
              ':response',
              ':storage',
              '@pypi//fastapi',
              '@pypi//flask',
              '@pypi//werkzeug'])

pytype_library(name='smtp_endpoint',
               srcs=['smtp_endpoint.py'],
               deps=['//koukan_cpython_smtplib:smtplib'])

pytype_library(name='gateway_lib',
               srcs=['gateway.py'],
               deps=[':fastapi_service',
                     ':hypercorn_main',
                     ':rest_endpoint',
                     ':smtp_endpoint',
                     ':smtp_service',
                     ':sync_filter_adapter',
                     ':rest_handler',
                     '@pypi//pyyaml'])

pytype_library(name='hello_policy',
               srcs=['hello_policy.py'],
               deps=[':recipient_router_filter'])

pytype_library(name='hello_filter',
               srcs=['hello_filter.py'],
               deps=[':filter'])

pytype_library(name='fake_smtpd',
               srcs=['fake_smtpd.py'])

pytype_library(name='ssmtp',
               srcs=['ssmtp.py'])

py_test(name='gateway_test',
        srcs=['gateway_test.py'],
        deps=[':gateway_lib',
              ':rest_endpoint',
              ':fake_smtpd',
              ':blob',
              '@pypi//parameterized',
              '@pypi//requests'])

pytype_library(name='postgres_test_utils',
               srcs=['postgres_test_utils.py'],
               deps=['@pypi//psycopg',
                     '@pypi//testing_postgresql'])

pytype_library(name='sqlite_test_utils',
               srcs=['sqlite_test_utils.py'])

py_test(name='end2end_test',
        srcs=['end2end_test.py'],
        data=['//config/local-test:router.yaml',
              '//config/local-test:gateway.yaml',
              '//examples/send_message:body.txt',
              '//testdata:multipart.msg',
              '//testdata:trivial.msg'],
        deps=[':router_service',
              ':gateway_lib',
              ':executor',
              ':hello_policy',
              ':hello_filter',
              ':ssmtp',
              ':fake_smtpd',
              ':postgres_test_utils',
              '//examples/send_message:send_message',
              '//examples/receiver:receiver',
              '//examples/receiver:fastapi_receiver',
              '@pypi//dkimpy',
              '@pypi//pyyaml'])

py_binary(name='router',
          main='router_service_main.py',
          srcs=['router_service_main.py'],
          deps=[':router_service'])
        
py_binary(name='gateway',
          main='gateway_main.py',
          srcs=['gateway_main.py'],
          deps=[':gateway_lib'])

test_suite(
    name='pytype_tests',
    tags=['pytype_test']
)

test_suite(
    name='all_tests',
    tags=['-broken']
)
