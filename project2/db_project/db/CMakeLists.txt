# Sources
set(DB_SOURCE_DIR src)
set(DB_SOURCES
  ${DB_SOURCE_DIR}/bpt.cc
  ${DB_SOURCE_DIR}/disk_space_manager/file.cc
  # Add your sources here
  # ${DB_SOURCE_DIR}/foo/bar/your_source.cc
  )

# Headers
set(DB_HEADER_DIR include)
set(DB_HEADERS
  ${DB_HEADER_DIR}/bpt.h
  ${DB_HEADER_DIR}/disk_space_manager/file.h
  ${DB_HEADER_DIR}/disk_space_manager/endian.h
  ${DB_HEADER_DIR}/gtest_printf.h
  # Add your headers here
  # ${DB_HEADER_DIR}/foo/bar/your_header.h
  )

add_library(db STATIC ${DB_HEADERS} ${DB_SOURCES})

target_include_directories(db
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${DB_HEADER_DIR}"
  )

