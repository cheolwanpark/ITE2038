#include <gtest/gtest.h>

#include <vector>

#include "disk_space_manager/database_file.h"
#include "disk_space_manager/file.h"

class DiskSpaceManagerTest : public ::testing::Test {
 protected:
  void SetUp(const char *filename) {
    _filename = filename;
    fd = file_open_database_file(_filename);
    ASSERT_TRUE(fd > 0);

    ASSERT_EQ(sizeof(page_t), kPageSize);
    ASSERT_EQ(sizeof(header_page_t), kPageSize);
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
  DatabaseFile file(fd);

  ASSERT_EQ(file.get_size(), kDefaultFileSize);

  uint64_t target_num_pages = kDefaultFileSize / kPageSize;
  header_page_t header_page;
  file.get_header_page(&header_page);
  ASSERT_EQ(header_page.num_of_pages, target_num_pages);

  uint64_t counted_num_pages = 1;
  pagenum_t next_page = header_page.first_free_page;
  page_t page;
  while (next_page != 0) {
    ++counted_num_pages;
    file.get_page(next_page, &page);
    next_page = page.next_free_page;
    ASSERT_TRUE(counted_num_pages <= target_num_pages);
  }
  ASSERT_EQ(counted_num_pages, target_num_pages);
}

TEST_F(DiskSpaceManagerTest, alloc_dealloc_pages) {
  SetUp("test_alloc_dealloc_pages.db");
  DatabaseFile file(fd);
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
  file.get_header_page(&header_page);
  for (int i = 0; i < header_page.num_of_pages - 1; ++i) {
    ASSERT_NE(file_alloc_page(fd), 0);
  }
  ASSERT_EQ(file.get_size(), kDefaultFileSize);
}

TEST_F(DiskSpaceManagerTest, auto_expand) {
  SetUp("test_auto_expand.db");
  DatabaseFile file(fd);

  auto original_size = file.get_size();

  header_page_t header_page;
  file.get_header_page(&header_page);
  for (int i = 0; i < header_page.num_of_pages; ++i) {
    ASSERT_NE(file_alloc_page(fd), 0);
  }

  auto expanded_size = original_size * 2;
  ASSERT_EQ(file.get_size(), expanded_size);
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