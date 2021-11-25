#include "trx.h"

#include <gtest/gtest.h>
#include <pthread.h>
#include <stdlib.h>

#include <algorithm>

#include "database.h"
#include "index_manager/index.h"
#include "log.h"

const int TRANSFER_COUNT = 10000;
const int SCAN_COUNT = 300;
const int TRANSFER_THREAD_NUM = 10;
const int SCAN_THREAD_NUM = 7;

const long long TABLE_NUMBER = 3;
const long long RECORD_NUMBER = 10000;
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
    // if (src_table_id == dest_table_id && src_record_id == dest_record_id)
    //   continue;

    // deadlock prevention
    if (src_table_id > dest_table_id) std::swap(src_table_id, dest_table_id);
    if (src_record_id > dest_record_id)
      std::swap(src_record_id, dest_record_id);

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

TEST_F(TrxTest, multi_thread) {
  SetUp("TT_multi_thread_test.db");
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