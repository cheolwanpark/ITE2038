#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "database.h"
#include "disk_space_manager/file.h"
#include "index_manager/index.h"
#include "log.h"
#include "trx.h"

const long long TABLE_NUMBER = 2;
const long long RECORD_NUMBER = 10000;

const int TRANSFER_COUNT = 10000;
const int SCAN_COUNT = 100;

const long long INITIAL_MONEY = 100000;
const int MAX_MONEY_TRANSFERRED = 100;
const long long SUM_MONEY = TABLE_NUMBER * RECORD_NUMBER * INITIAL_MONEY;

const char* FILENAME = "single_thread_test.db";

union account_t {
  long long money;
  char data[100];
};

int main(int argc, char** argv) {
  auto start = clock();
  char filename[TABLE_NUMBER][100];
  int64_t table_id[TABLE_NUMBER];
  init_db(30000);
  for (int i = 0; i < TABLE_NUMBER; ++i) {
    sprintf(filename[i], "%d_%s", i, FILENAME);
    remove(filename[i]);
    table_id[i] = file_open_table_file(filename[i]);
  }

  for (int i = 0; i < TABLE_NUMBER; ++i) {
    remove(filename[i]);
  }

  srand(time(NULL));

  // initialize accounts
  account_t acc;
  acc.money = INITIAL_MONEY;
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      if (db_insert(table_id[tid], rid, acc.data, sizeof(acc))) {
        LOG_ERR("failed to insert!");
        return -1;
      }
    }
  }
  LOG_INFO("initialization done");

  // transfer phase
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

    money_transferred = rand() % MAX_MONEY_TRANSFERRED;
    money_transferred =
        rand() % 2 == 0 ? -money_transferred : money_transferred;

    auto trx = trx_begin();
    account_t acc1, acc2;
    uint16_t size;

    if (db_find(src_table_id, src_record_id, acc1.data, &size, trx)) {
      LOG_ERR("find failed!");
      return -1;
    }
    if (size != sizeof(acc1)) {
      LOG_ERR("invalid result!");
      return -1;
    }

    acc1.money -= money_transferred;
    if (db_update(src_table_id, src_record_id, acc1.data, sizeof(acc1), &size,
                  trx)) {
      LOG_ERR("update failed!");
      return -1;
    }
    if (size != sizeof(acc1)) {
      LOG_ERR("invalid result!");
      return -1;
    }

    if (db_find(dest_table_id, dest_record_id, acc2.data, &size, trx)) {
      LOG_ERR("find failed!");
      return -1;
    }
    if (size != sizeof(acc2)) {
      LOG_ERR("invalid result!");
      return -1;
    }

    acc2.money += money_transferred;
    if (db_update(dest_table_id, dest_record_id, acc2.data, sizeof(acc2), &size,
                  trx)) {
      LOG_ERR("update failed!");
      return -1;
    }
    if (size != sizeof(acc2)) {
      LOG_ERR("invalid result!");
      return -1;
    }

    if (rand() % 2) {
      if (trx_commit(trx) != trx) {
        LOG_ERR("commit failed!");
        return -1;
      }
    } else {
      if (trx_abort(trx) != trx) {
        LOG_ERR("abort failed!");
        return -1;
      }
    }

    if ((i + 1) % 5000 == 0) LOG_INFO("%dth transfer complete", i + 1);
  }
  LOG_INFO("Transfer done.");

  // scan phase
  for (int scan = 0; scan < SCAN_COUNT; ++scan) {
    long long sum_money = 0;
    auto trx = trx_begin();
    int aborted = false;
    for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
      for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
        account_t acc;
        uint16_t size;
        if (db_find(table_id[tid], rid, acc.data, &size, trx)) {
          LOG_ERR("find failed!");
          return -1;
        }
        sum_money += acc.money;
      }
    }
    if (trx_commit(trx) != trx) {
      LOG_ERR("commit failed");
      return -1;
    }
    if (sum_money != SUM_MONEY) {
      LOG_ERR("inconsistent state is detected!");
      return -1;
    }
    if ((scan + 1) % 50 == 0) LOG_INFO("%dth scan done", scan + 1);
  }
  LOG_INFO("Scan is done.");

  auto time = clock() - start;
  LOG_INFO("complete in %llf seconds", (double)time / CLOCKS_PER_SEC);
}
