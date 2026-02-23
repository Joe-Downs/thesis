find_package(MPI REQUIRED)

add_executable(concept concept.c)

target_compile_features(concept PRIVATE c_std_11)

target_link_libraries(concept
    PRIVATE
        openzl
        MPI::MPI_C
)
