include_directories ( ${cs739project_SOURCE_DIR}/src )

add_library (Util Util.cpp)
add_library (MapFetcher MapFetcher.cpp)
add_library (Index Index.cpp)
add_executable (test.x test.cpp)
add_executable (player player.cpp)
target_link_libraries(player Index MapFetcher Util)
