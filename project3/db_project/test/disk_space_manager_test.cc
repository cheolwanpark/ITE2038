#include <gtest/gtest.h>

#include <vector>

#include "disk_space_manager/file.h"
#include "log.h"

class DiskSpaceManagerTest : public ::testing::Test {
 protected:
  void SetUp(const char *filename) {
    _filename = filename;
    init_db(100);
    table_id = file_open_table_file(_filename);
    ASSERT_TRUE(table_id > 0);
  }

  void TearDown() override {
    file_close_table_files();
    remove(_filename);
  }

  const char *_filename;
  int64_t table_id;
};

TEST_F(DiskSpaceManagerTest, open_db_file) {
  SetUp("DSM_open_db_file_test.db");

  ASSERT_EQ(file_size(table_id), kDefaultFileSize);

  uint64_t target_num_pages = kDefaultFileSize / kPageSize;
  header_page_t header_page;
  file_read_header_page(table_id, &header_page);
  ASSERT_EQ(header_page.header.num_of_pages, target_num_pages);
}

TEST_F(DiskSpaceManagerTest, alloc_dealloc_pages) {
  SetUp("DSM_alloc_dealloc_pages_test.db");
  uint32_t allocating_pages = 1234;

  std::vector<pagenum_t> allocated_pages;
  for (int i = 0; i < allocating_pages; ++i) {
    auto allocated_page = file_alloc_page(table_id);
    ASSERT_NE(allocated_page, 0);
    allocated_pages.push_back(allocated_page);
  }

  for (auto page : allocated_pages) {
    file_free_page(table_id, page);
  }

  header_page_t header_page;
  file_read_header_page(table_id, &header_page);
  for (int i = 0; i < header_page.header.num_of_pages - 1; ++i) {
    ASSERT_NE(file_alloc_page(table_id), 0);
  }
  ASSERT_EQ(file_size(table_id), kDefaultFileSize);
}

TEST_F(DiskSpaceManagerTest, auto_expand) {
  SetUp("DSM_auto_expand_test.db");

  auto original_size = file_size(table_id);

  header_page_t header_page;
  file_read_header_page(table_id, &header_page);
  for (int i = 0; i < header_page.header.num_of_pages; ++i) {
    ASSERT_NE(file_alloc_page(table_id), 0);
  }

  auto expanded_size = original_size * 2;
  ASSERT_EQ(file_size(table_id), expanded_size);
  for (int i = 0; i < 1000; ++i) {
    ASSERT_NE(file_alloc_page(table_id), 0);
  }
}

TEST_F(DiskSpaceManagerTest, read_write_page) {
  SetUp("DSM_read_write_page_test.db");

  auto allocated_page = file_alloc_page(table_id);
  page_t page;
  file_read_page(table_id, allocated_page, &page);
  strcpy(page.data, "Hello World!");
  file_write_page(table_id, allocated_page, &page);

  file_close_table_files();
  table_id = file_open_table_file(_filename);
  ASSERT_TRUE(table_id > 0) << "fd is " << table_id;
  file_read_page(table_id, allocated_page, &page);
  ASSERT_TRUE(strcmp(page.data, "Hello World!") == 0);
}

TEST_F(DiskSpaceManagerTest, read_write_header) {
  SetUp("DSM_read_write_header_test.db");

  auto t1 = file_alloc_page(table_id);
  page_t page;
  strcpy(page.data, "Hello World!");
  file_write_page(table_id, t1, &page);

  pagenum_t val1 = 54321;
  uint64_t val2 = 12345;
  pagenum_t val3 = 321123;

  header_page_t header;
  header.header.first_free_page = val1;
  header.header.num_of_pages = val2;
  header.header.root_page_number = val3;
  file_write_header_page(table_id, &header);

  memset(&header, 0, sizeof(header));
  file_read_header_page(table_id, &header);
  ASSERT_EQ(header.header.first_free_page, val1);
  ASSERT_EQ(header.header.num_of_pages, val2);
  ASSERT_EQ(header.header.root_page_number, val3);
}