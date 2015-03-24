# Right now, the tests are built using the static version of the
# replication listener, but we need to decide which one to use.

set(MySQL_TRANSPORT_TEST test-transport)
set(MySQL_BASIC_TEST test-basic)
set(MySQL_REPLAYBINLOG_TEST replaybinlog)
set(MySQL_REPLAY_SYS_VARS_TEST replay_sys_vars)

foreach(test ${MySQL_TRANSPORT_TEST} ${MySQL_BASIC_TEST} ${MySQL_REPLAYBINLOG_TEST} ${MySQL_REPLAY_SYS_VARS_TEST})
  message("Adding test ${test}")
  add_executable(${test} ${test}.cpp)
  target_link_libraries(${test} replication_static gtest)
endforeach()

add_test(TransportTest ${MySQL_TRANSPORT_TEST})

if(WITH_SERVER_TESTS)
  add_test(BasicTest ${MySQL_BASIC_TEST})
endif(WITH_SERVER_TESTS)

if(MySQL_TEST_SERVER_URL)
  add_test(ReplaybinlogTest ${MySQL_REPLAYBINLOG_TEST} ${MySQL_TEST_SERVER_URL})
  add_test(ReplaySysVarsTest ${MySQL_REPLAY_SYS_VARS_TEST})
else(MySQL_TEST_SERVER_URL)
  message(
    "MySQL test server url is not specified. The test cases with the server will be skipped.\n"
    "You can run cmake with DMySQL_TEST_SERVER_URL option to test with server access.\n"
    "  ex) cmake . DMySQL_TEST_SERVER_URL=mysql://root:@127.0.0.1:3306 ")
endif(MySQL_TEST_SERVER_URL)
