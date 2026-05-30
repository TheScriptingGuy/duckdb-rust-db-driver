cmake_minimum_required(VERSION 3.20)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(rust_db_driver)

set(EXTENSION_NAME rust_db_driver)

# extension-ci-tools always calls cmake --build with Release profile.
set(CARGO_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/target/release")

if(WIN32)
    set(CARGO_LIB "${CARGO_OUT_DIR}/${EXTENSION_NAME}.dll")
elseif(APPLE)
    set(CARGO_LIB "${CARGO_OUT_DIR}/lib${EXTENSION_NAME}.dylib")
else()
    set(CARGO_LIB "${CARGO_OUT_DIR}/lib${EXTENSION_NAME}.so")
endif()

# extension-ci-tools expects the artifact at:
#   <cmake_binary_dir>/extension/<name>/<name>.duckdb_extension
set(EXT_DIR "${CMAKE_BINARY_DIR}/extension/${EXTENSION_NAME}")
set(EXT_FILE "${EXT_DIR}/${EXTENSION_NAME}.duckdb_extension")

file(MAKE_DIRECTORY "${EXT_DIR}")

find_program(CARGO_EXE cargo REQUIRED)

add_custom_command(
    OUTPUT "${EXT_FILE}"
    COMMAND "${CARGO_EXE}" build --release --lib
    COMMAND "${CMAKE_COMMAND}" -E copy "${CARGO_LIB}" "${EXT_FILE}"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Building ${EXTENSION_NAME} Rust extension via cargo"
    VERBATIM
)

add_custom_target(${EXTENSION_NAME} ALL DEPENDS "${EXT_FILE}")
