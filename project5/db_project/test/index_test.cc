#include "index_manager/index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "database.h"
#include "log.h"

const int DUMMY_TRX = -1;
const int NUM_BUF = 100000;
const int INSERTING_N = 100000;

class IndexTest : public ::testing::Test {
 protected:
  void SetUp(const char *filename) {
    strcpy(_filename, filename);
    init_db(NUM_BUF);
    table_id = open_table(_filename);
    ASSERT_TRUE(table_id > 0);
  }

  void TearDown() override {
    shutdown_db();
    remove(_filename);
  }

  char _filename[256];
  int64_t table_id;
};

TEST_F(IndexTest, insert_and_find) {
  SetUp("IT_insert_and_find_test.db");

  const int kinds = 4;
  char vals[kinds][112] = {
      "Hello World!",
      "My name is DBMS!",
      "BPT is dynamic index!",
      "disk is managed as page!",
  };
  uint16_t sizes[kinds] = {50, 70, 100, 112};

  uint32_t inserting_cnt = INSERTING_N;
  std::vector<int> keys;
  for (int i = 1; i <= inserting_cnt; ++i) {
    keys.emplace_back(i);
  }
  std::random_device rd;
  std::default_random_engine rng(rd());
  std::shuffle(keys.begin(), keys.end(), rng);
  std::set<int> test(keys.begin(), keys.end());
  ASSERT_EQ(keys.size(), test.size());

  char read_buf[112];
  uint16_t size;
  for (auto key : keys) {
    ASSERT_NE(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0);
    ASSERT_EQ(db_insert(table_id, key, vals[key % kinds], sizes[key % kinds]),
              0)
        << "failed to insert " << key;
  }
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";

  for (auto key : keys) {
    ASSERT_EQ(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0)
        << "failed to find " << key;
    ASSERT_EQ(size, sizes[key % kinds])
        << "size of key = " << key << " is invalid";
    ASSERT_TRUE(strcmp(read_buf, vals[key % kinds]) == 0)
        << "data of key = " << key << " is invalid";
  }
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";
}

TEST_F(IndexTest, insert_and_delete_all) {
  SetUp("IT_insert_and_delete_all_test.db");

  const int kinds = 4;
  char vals[kinds][112] = {
      "Hello World!",
      "My name is DBMS!",
      "BPT is dynamic index!",
      "disk is managed as page!",
  };
  uint16_t sizes[kinds] = {50, 70, 100, 112};

  uint32_t inserting_cnt = INSERTING_N;
  std::vector<int> keys;
  for (int i = 1; i <= inserting_cnt; ++i) {
    keys.emplace_back(i);
  }
  std::random_device rd;
  std::default_random_engine rng(rd());
  std::shuffle(keys.begin(), keys.end(), rng);
  std::set<int> test(keys.begin(), keys.end());
  ASSERT_EQ(keys.size(), test.size());

  for (auto key : keys) {
    ASSERT_EQ(db_insert(table_id, key, vals[key % kinds], sizes[key % kinds]),
              0)
        << "failed to insert " << key;
  }
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";
  LOG_INFO("insert complete!");

  shutdown_db();
  init_db(NUM_BUF);
  table_id = open_table(_filename);
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";

  for (auto key : keys) {
    ASSERT_EQ(db_delete(table_id, key), 0) << "failed to delete " << key;
    // ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";
  }
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";
}

TEST_F(IndexTest, insert_delete_find_update) {
  SetUp("IT_insert_delete_find_test.db");

  const int kinds = 4;
  char vals[kinds][112] = {
      "Hello World!",
      "My name is DBMS!",
      "BPT is dynamic index!",
      "disk is managed as page!",
  };
  uint16_t sizes[kinds] = {50, 70, 100, 112};

  uint32_t inserting_cnt = INSERTING_N;
  std::vector<int> keys;
  for (int i = 1; i <= inserting_cnt; ++i) {
    keys.emplace_back(i);
  }
  std::random_device rd;
  std::default_random_engine rng(rd());
  std::shuffle(keys.begin(), keys.end(), rng);
  std::set<int> test(keys.begin(), keys.end());
  ASSERT_EQ(keys.size(), test.size());

  for (auto key : keys) {
    ASSERT_EQ(db_insert(table_id, key, vals[key % kinds], sizes[key % kinds]),
              0)
        << "failed to insert " << key;
  }
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";

  char read_buf[112];
  uint16_t size;
  for (int i = 0; i < keys.size(); ++i) {
    auto key = keys[i];
    ASSERT_EQ(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0)
        << "failed to find " << key;
    ASSERT_EQ(size, sizes[key % kinds])
        << "size of key = " << key << " is invalid";
    ASSERT_TRUE(strcmp(read_buf, vals[key % kinds]) == 0)
        << "data of key = " << key << " is invalid";
  }

  for (int i = 0; i < keys.size() / 2; ++i) {
    auto key = keys[i];
    ASSERT_EQ(db_delete(table_id, key), 0);
  }
  for (int i = keys.size() / 2; i < keys.size(); ++i) {
    if (i % 3 == 0) {
      auto key = keys[i];
      ASSERT_EQ(db_delete(table_id, key), 0);
    }
  }
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";

  shutdown_db();
  init_db(NUM_BUF);
  table_id = open_table(_filename);
  ASSERT_TRUE(is_clean(table_id)) << "tree is invalid";

  for (int i = 0; i < keys.size() / 2; ++i) {
    auto key = keys[i];
    ASSERT_NE(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0);
  }
  for (int i = keys.size() / 2; i < keys.size(); ++i) {
    auto key = keys[i];
    if (i % 3 == 0) {
      ASSERT_NE(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0);
    } else {
      ASSERT_EQ(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0)
          << "failed to find " << key;
      ASSERT_EQ(size, sizes[key % kinds])
          << "size of key = " << key << " is invalid";
      ASSERT_TRUE(strcmp(read_buf, vals[key % kinds]) == 0)
          << "data of key = " << key << " is invalid";
    }
  }

  for (int i = keys.size() / 2; i < keys.size(); ++i) {
    auto key = keys[i];
    if (i % 3 == 0) {
      ASSERT_NE(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0);
    } else {
      auto upd_key = keys[(i + 1) % keys.size()];
      ASSERT_EQ(db_update(table_id, key, vals[upd_key % kinds],
                          sizes[upd_key % kinds], &size, DUMMY_TRX),
                0)
          << "failed to update " << key;
    }
  }
  for (int i = keys.size() / 2; i < keys.size(); ++i) {
    auto key = keys[i];
    if (i % 3 == 0) {
      ASSERT_NE(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0);
    } else {
      auto upd_key = keys[(i + 1) % keys.size()];
      ASSERT_EQ(db_find(table_id, key, read_buf, &size, DUMMY_TRX), 0)
          << "failed to find " << key;
      ASSERT_EQ(size, sizes[key % kinds])
          << "size of key = " << key << " is invalid";
      ASSERT_TRUE(
          strncmp(read_buf, vals[upd_key % kinds], sizes[upd_key % kinds]) == 0)
          << "data of key = " << key << " is invalid";
    }
  }
}