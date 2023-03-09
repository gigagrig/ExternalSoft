cmake_minimum_required(VERSION 3.23)
project(SortFile)

set(CMAKE_CXX_STANDARD 17)

add_executable(SortFile main.cpp)

target_link_libraries(SortFile pthread)





