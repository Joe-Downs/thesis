cmake_minimum_required(VERSION 3.20)
project(Thesis)


# =================================== OpenZL ===================================
# Options must be set before add_subdirectory so they take effect.
set(OPENZL_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_CLI OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_PYTHON_EXT OFF CACHE BOOL "" FORCE)
set(OPENZL_BUILD_PYTHON_DEMO OFF CACHE BOOL "" FORCE)
set(OPENZL_ALLOW_INTROSPECTION OFF CACHE BOOL "" FORCE)
set(OPENZL_INSTALL OFF CACHE BOOL "" FORCE)

add_subdirectory(modules/openzl)

# ==================================== Code ====================================
add_subdirectory(code/concept)
