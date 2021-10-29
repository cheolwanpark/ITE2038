#include "index_manager/bpt.h"

#include <gtest/gtest.h>

#include <string>

#include "buffer_manager.h"
#include "database.h"
#include "disk_space_manager/file.h"
#include "log.h"

class BptTest : public ::testing::Test {
 protected:
  void SetUp(const char *filename) {
    _filename = filename;
    init_db(100);
    table_id = file_open_table_file(_filename);
    ASSERT_TRUE(table_id > 0);
    root = 0;
  }

  void TearDown() override {
    shutdown_db();
    remove(_filename);
  }

  const char *_filename;
  int64_t table_id;
  pagenum_t root;
};

TEST_F(BptTest, insert_and_find) {
  SetUp("BT_insert_and_find_test.db");

  const int kinds = 4;
  char vals[kinds][50] = {
      "Hello World!",
      "My name is DBMS!",
      "BPT is dynamic index!",
      "disk is managed as page!",
  };

  uint32_t inserting_cnt = 10000;
  for (int i = inserting_cnt; i > 0; --i) {
    root = bpt_insert(table_id, root, i, 50, vals[i % kinds]);
    ASSERT_NE(root, 0);
  }
  ASSERT_TRUE(is_clean(table_id, root));

  char read_buf[112];
  uint16_t size;
  for (int i = 1; i <= inserting_cnt; ++i) {
    ASSERT_TRUE(bpt_find(table_id, root, i, &size, read_buf))
        << "failed to find " << i;
    ASSERT_EQ(size, 50) << "size of key = " << i << " is invalid";
    ASSERT_TRUE(strcmp(read_buf, vals[i % kinds]) == 0)
        << "data of key = " << i << " is invalid";
  }
}

TEST_F(BptTest, insert_delete_find) {
  SetUp("BT_insert_delete_find_test.db");

  const int kinds = 4;
  char vals[kinds][50] = {
      "Hello World!",
      "My name is DBMS!",
      "BPT is dynamic index!",
      "disk is managed as page!",
  };

  uint32_t inserting_cnt = 10000;
  for (int i = inserting_cnt; i > 0; --i) {
    root = bpt_insert(table_id, root, i, 50, vals[i % kinds]);
    ASSERT_NE(root, 0);
  }

  char read_buf[112];
  uint16_t size;
  for (int i = 1; i <= inserting_cnt; ++i) {
    ASSERT_TRUE(bpt_find(table_id, root, i, &size, read_buf))
        << "failed to find " << i;
    ASSERT_EQ(size, 50) << "size of key = " << i << " is invalid";
    ASSERT_TRUE(strcmp(read_buf, vals[i % kinds]) == 0)
        << "data of key = " << i << " is invalid";
  }

  for (int i = 1; i < inserting_cnt / 2; ++i) {
    int bef = count_free_frames();
    root = bpt_delete(table_id, root, i);
    ASSERT_NE(root, 0);
    if (bef != count_free_frames()) {
      LOG_INFO("free frames decreased: %d", count_free_frames());
    }
  }
  for (int i = inserting_cnt / 2; i <= inserting_cnt; ++i) {
    if (i % 3 == 0) {
      int bef = count_free_frames();
      root = bpt_delete(table_id, root, i);
      ASSERT_NE(root, 0);
      if (bef != count_free_frames()) {
        LOG_INFO("free frames decreased: %d", count_free_frames());
      }
    }
  }
  LOG_INFO("free frames: %d", count_free_frames());

  for (int i = 0; i < inserting_cnt / 2; ++i) {
    ASSERT_FALSE(bpt_find(table_id, root, i, &size, read_buf));
  }
  for (int i = inserting_cnt / 2; i <= inserting_cnt; ++i) {
    if (i % 3 == 0) {
      ASSERT_FALSE(bpt_find(table_id, root, i, &size, read_buf));
    } else {
      ASSERT_TRUE(bpt_find(table_id, root, i, &size, read_buf))
          << "failed to find " << i;
      ASSERT_EQ(size, 50) << "size of key = " << i << " is invalid";
      ASSERT_TRUE(strcmp(read_buf, vals[i % kinds]) == 0)
          << "data of key = " << i << " is invalid";
    }
  }
}