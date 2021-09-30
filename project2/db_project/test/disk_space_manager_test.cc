#include <gtest/gtest.h>

#include <vector>

#include "disk_space_manager/file.h"
#include "gtest_printf.h"

class DiskSpaceManagerTest : public ::testing::Test {
 protected:
  void SetUp(const char *filename) {
    _filename = filename;
    fd = file_open_database_file(_filename);
    ASSERT_TRUE(fd > 0);

    ASSERT_EQ(sizeof(page_t), kPageSize);
  }

  void TearDown() override {
    file_close_database_file();
    remove(_filename);
  }

  const char *_filename;
  int fd;
};

TEST_F(DiskSpaceManagerTest, open_db_file) {
  SetUp("test_open_db_file.db");

  ASSERT_EQ(file_size(fd), kDefaultFileSize);

  uint64_t target_num_pages = kDefaultFileSize / kPageSize;
  header_page_t header_page;
  file_read_header_page(fd, &header_page);
  ASSERT_EQ(header_page.header.num_of_pages, target_num_pages);
}

TEST_F(DiskSpaceManagerTest, alloc_dealloc_pages) {
  SetUp("test_alloc_dealloc_pages.db");
  uint32_t allocating_pages = 1234;

  std::vector<pagenum_t> allocated_pages;
  for (int i = 0; i < allocating_pages; ++i) {
    auto allocated_page = file_alloc_page(fd);
    ASSERT_NE(allocated_page, 0);
    allocated_pages.push_back(allocated_page);
  }

  for (auto page : allocated_pages) {
    file_free_page(fd, page);
  }

  header_page_t header_page;
  file_read_header_page(fd, &header_page);
  for (int i = 0; i < header_page.header.num_of_pages - 1; ++i) {
    ASSERT_NE(file_alloc_page(fd), 0);
  }
  ASSERT_EQ(file_size(fd), kDefaultFileSize);
}

TEST_F(DiskSpaceManagerTest, auto_expand) {
  SetUp("test_auto_expand.db");

  auto original_size = file_size(fd);

  header_page_t header_page;
  file_read_header_page(fd, &header_page);
  for (int i = 0; i < header_page.header.num_of_pages; ++i) {
    ASSERT_NE(file_alloc_page(fd), 0);
  }

  auto expanded_size = original_size * 2;
  ASSERT_EQ(file_size(fd), expanded_size);
  for (int i = 0; i < 1000; ++i) {
    ASSERT_NE(file_alloc_page(fd), 0);
  }
}

TEST_F(DiskSpaceManagerTest, read_write_page) {
  SetUp("test_read_write_page.db");

  auto allocated_page = file_alloc_page(fd);
  page_t page;
  file_read_page(fd, allocated_page, &page);
  strcpy(page.data, "Hello World!");
  file_write_page(fd, allocated_page, &page);

  file_close_database_file();
  fd = file_open_database_file(_filename);
  file_read_page(fd, allocated_page, &page);
  ASSERT_TRUE(strcmp(page.data, "Hello World!") == 0);
}