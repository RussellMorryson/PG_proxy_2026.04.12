cmake_minimum_required(VERSION 3.10)
project(PgProxy)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

add_executable(pg_proxy main.cpp)