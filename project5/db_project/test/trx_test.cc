#include "trx.h"

#include <gtest/gtest.h>
#include <pthread.h>
#include <stdlib.h>

#include <algorithm>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "database.h"
#include "index_manager/index.h"
#include "log.h"

const long long TABLE_NUMBER = 3;
const long long RECORD_NUMBER = 2000;

const int TRANSFER_COUNT = 5000;
const int SCAN_COUNT = 300;
const int TRANSFER_THREAD_NUM = 4;
const int SCAN_THREAD_NUM = 3;

const long long INITIAL_MONEY = 100000;
const int MAX_MONEY_TRANSFERRED = 100;
const long long SUM_MONEY = TABLE_NUMBER * RECORD_NUMBER * INITIAL_MONEY;

class TrxTest : public ::testing::Test {
 protected:
  void SetUp(const char *filename) {
    init_db(10000);
    for (int i = 0; i < TABLE_NUMBER; ++i) {
      sprintf(_filename[i], "%d_%s", i, filename);
      table_id[i] = file_open_table_file(_filename[i]);
      ASSERT_TRUE(table_id[i] > 0);
    }
  }

  void TearDown() override {
    shutdown_db();
    for (int i = 0; i < TABLE_NUMBER; ++i) {
      remove(_filename[i]);
    }
  }

  char _filename[TABLE_NUMBER][100];
  int64_t table_id[TABLE_NUMBER];
};

// Bank account test (mixed locks)
union account_t {
  long long money;
  char data[100];
};

bool failed = false;

void __transfer_thread_func(void *arg) {
  int64_t table_id[TABLE_NUMBER];
  memcpy(table_id, arg, sizeof(table_id));

  int64_t src_table_id, src_record_id, dest_table_id, dest_record_id;
  long long money_transferred;

  for (int i = 0; i < TRANSFER_COUNT; i++) {
    src_table_id = table_id[rand() % TABLE_NUMBER];
    src_record_id = rand() % RECORD_NUMBER;
    dest_table_id = table_id[rand() % TABLE_NUMBER];
    dest_record_id = rand() % RECORD_NUMBER;

    // avoid transfer to same account
    if (src_table_id == dest_table_id && src_record_id == dest_record_id)
      continue;

    // // deadlock prevention
    // if (src_table_id > dest_table_id) std::swap(src_table_id, dest_table_id);
    // if (src_record_id > dest_record_id)
    //   std::swap(src_record_id, dest_record_id);

    money_transferred = rand() % MAX_MONEY_TRANSFERRED;
    money_transferred =
        rand() % 2 == 0 ? -money_transferred : money_transferred;

    auto trx = trx_begin();
    account_t acc1, acc2;
    uint16_t size;

    if (db_find(src_table_id, src_record_id, acc1.data, &size, trx)) continue;
    ASSERT_EQ(size, sizeof(acc1));

    acc1.money -= money_transferred;
    if (db_update(src_table_id, src_record_id, acc1.data, sizeof(acc1), &size,
                  trx))
      continue;
    ASSERT_EQ(size, sizeof(acc1));

    if (db_find(dest_table_id, dest_record_id, acc2.data, &size, trx)) continue;
    ASSERT_EQ(size, sizeof(acc2));

    acc2.money += money_transferred;
    if (db_update(dest_table_id, dest_record_id, acc2.data, sizeof(acc2), &size,
                  trx))
      continue;
    ASSERT_EQ(size, sizeof(acc2));

    if (rand() % 2)
      ASSERT_EQ(trx_commit(trx), trx);
    else {
      ASSERT_EQ(trx_abort(trx), trx);
    }
    if (failed) {
      return;
    }

    if ((i + 1) % 1000 == 0)
      LOG_INFO("%dth transfer complete in %d", i + 1, pthread_self());
  }
  GPRINTF("Transfer thread is done.");
}

void *transfer_thread_func(void *arg) {
  __transfer_thread_func(arg);
  return NULL;
}

void __scan_thread_func(void *arg) {
  int64_t table_id[TABLE_NUMBER];
  memcpy(table_id, arg, sizeof(table_id));

  for (int scan = 0; scan < SCAN_COUNT; ++scan) {
    long long sum_money = 0;
    auto trx = trx_begin();
    int aborted = false;
    for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
      for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
        account_t acc;
        uint16_t size;
        if (db_find(table_id[tid], rid, acc.data, &size, trx)) {
          aborted = true;
          break;
        }
        sum_money += acc.money;
      }
      if (aborted) break;
    }
    if (failed) {
      return;
    }
    if (!aborted) {
      ASSERT_EQ(trx_commit(trx), trx);
      if (sum_money != SUM_MONEY) failed = true;
      ASSERT_EQ(sum_money, SUM_MONEY)
          << "Inconsistent state is detected in " << scan + 1 << "th scan!!";
    }
    if ((scan + 1) % 100 == 0)
      LOG_INFO("%dth scan done in %d", scan + 1, pthread_self());
  }
  GPRINTF("Scan thread is done.");
}

void *scan_thread_func(void *arg) {
  __scan_thread_func(arg);
  return NULL;
}

TEST_F(TrxTest, mixed) {
  SetUp("TT_mixed_test.db");
  pthread_t transfer_threads[TRANSFER_THREAD_NUM];
  pthread_t scan_threads[SCAN_THREAD_NUM];

  srand(time(NULL));

  // initialize accounts
  account_t acc;
  acc.money = INITIAL_MONEY;
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      ASSERT_EQ(db_insert(table_id[tid], rid, acc.data, sizeof(acc)), 0);
    }
  }
  GPRINTF("initialization done.");

  for (int i = 0; i < TRANSFER_THREAD_NUM; ++i) {
    pthread_create(&transfer_threads[i], 0, transfer_thread_func,
                   (void *)table_id);
  }
  for (int i = 0; i < SCAN_THREAD_NUM; ++i) {
    pthread_create(&scan_threads[i], 0, scan_thread_func, (void *)table_id);
  }

  for (int i = 0; i < TRANSFER_THREAD_NUM; ++i) {
    pthread_join(transfer_threads[i], NULL);
  }
  for (int i = 0; i < SCAN_THREAD_NUM; ++i) {
    pthread_join(scan_threads[i], NULL);
  }
  GPRINTF("complete!");
}

// S lock only test
const int SCANNING_THREAD_NUM = 100;
const int SCANNING_COUNT = 30;
const int kinds = 7;
char vals[kinds][112] = {
    "Hello World!",
    "My name is DBMS!",
    "BPT is dynamic index!",
    "disk is managed as page!",
    "hfdjshfksdhfksdhfkdsjhfkshfkjhsdkjfhksa",
    "hgjsdhgdshpqiqowhoqiwrjqoijeqnlgdsghosghsdjghsdkjghhoq",
    "13512uo1ut018ugjog10gu310ijonf13ijgioflfm!fo13t0",
};
uint16_t sizes[kinds] = {50, 70, 100, 112, 112, 112, 112};

void __scanning_func(void *arg) {
  int64_t table_id[TABLE_NUMBER];
  memcpy(table_id, arg, sizeof(table_id));

  std::vector<std::pair<int64_t, int64_t>> keys;
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < TABLE_NUMBER; ++rid) {
      keys.emplace_back(std::make_pair(tid, rid));
    }
  }
  std::random_device rd;
  std::default_random_engine rng(rd());

  char read_buf[112];
  uint16_t size;
  for (int scan = 0; scan < SCANNING_COUNT; ++scan) {
    std::shuffle(keys.begin(), keys.end(), rng);
    auto trx = trx_begin();
    for (auto &key : keys) {
      auto id = key.first + key.second;
      ASSERT_EQ(db_find(table_id[key.first], key.second, read_buf, &size, trx),
                0);
      ASSERT_EQ(size, sizes[id % kinds]);
      ASSERT_TRUE(strcmp(read_buf, vals[id % kinds]) == 0);
    }
    ASSERT_EQ(trx_commit(trx), trx);
  }
}

void *scanning_func(void *arg) {
  __scanning_func(arg);
  return NULL;
}

TEST_F(TrxTest, s_lock_only) {
  SetUp("TT_s_lock_only_test.db");
  pthread_t scanning_threads[SCANNING_THREAD_NUM];

  srand(time(NULL));

  // initialize accounts
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      auto id = tid + rid;
      ASSERT_EQ(
          db_insert(table_id[tid], rid, vals[id % kinds], sizes[id % kinds]),
          0);
    }
  }
  GPRINTF("initialization done.");

  for (int i = 0; i < SCANNING_THREAD_NUM; ++i) {
    pthread_create(&scanning_threads[i], 0, scanning_func, (void *)table_id);
  }

  for (int i = 0; i < SCANNING_THREAD_NUM; ++i) {
    pthread_join(scanning_threads[i], NULL);
  }
  GPRINTF("complete!");
}

// X lock only test
const int UPDATING_THREAD_NUM = 30;
const int UPDATING_COUNT = 1000;

void __updating_func(void *arg) {
  int64_t table_id[TABLE_NUMBER];
  memcpy(table_id, arg, sizeof(table_id));

  char read_buf[112];
  uint16_t size;
  std::vector<int> rids;
  for (int iter = 0; iter < UPDATING_COUNT; ++iter) {
    int tid = rand() % TABLE_NUMBER;
    auto trx = trx_begin();

    auto n = rand() % 5 + 3;
    rids.clear();
    for (int i = 0; i < n; ++i) rids.push_back(rand() % RECORD_NUMBER);
    std::sort(rids.begin(), rids.end());

    for (auto rid : rids) {
      auto id = rid + tid;
      ASSERT_EQ(db_update(table_id[tid], rid, vals[id % kinds],
                          sizes[id % kinds], &size, trx),
                0);
      ASSERT_EQ(size, sizes[id % kinds]);
    }
    ASSERT_EQ(trx_commit(trx), trx);
    if ((iter + 1) % 100 == 0) LOG_INFO("iteration %d done", (iter + 1));
  }
}

void *updating_func(void *arg) {
  __updating_func(arg);
  return NULL;
}

TEST_F(TrxTest, x_lock_only) {
  SetUp("TT_x_lock_only_test.db");
  pthread_t updating_threads[UPDATING_THREAD_NUM];

  srand(time(NULL));

  // initialize accounts
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      auto id = tid + rid;
      ASSERT_EQ(
          db_insert(table_id[tid], rid, vals[id % kinds], sizes[id % kinds]),
          0);
    }
  }
  GPRINTF("initialization done.");

  for (int i = 0; i < UPDATING_THREAD_NUM; ++i) {
    pthread_create(&updating_threads[i], 0, updating_func, (void *)table_id);
  }

  for (int i = 0; i < UPDATING_THREAD_NUM; ++i) {
    pthread_join(updating_threads[i], NULL);
  }
  GPRINTF("complete!");
}