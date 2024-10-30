#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "database.h"
#include "disk_space_manager/file.h"
#include "index_manager/index.h"
#include "log.h"
#include "recovery.h"
#include "trx.h"

const int TRANSFER_THREAD_NUM = 10;
const int SCAN_THREAD_NUM = 3;
const int MULTI_THREAD_BUFFER_SIZE = TRANSFER_THREAD_NUM + SCAN_THREAD_NUM;

const long long TABLE_NUMBER = 4;
const long long RECORD_NUMBER = 10000;

const int TRANSFER_COUNT = 5000;
const int SCAN_COUNT = 10000;

const int LONG_TRX_TEST_BUF_SIZE = 100;
const int TRANSFER_PER_TRX_IN_LONG_TRX = 100;

const long long INITIAL_MONEY = 100000;
const int MAX_MONEY_TRANSFERRED = 100;
const long long SUM_MONEY = TABLE_NUMBER * RECORD_NUMBER * INITIAL_MONEY;

char LOG_FILENAME[100] = "log.txt";
char LOGMSG_FILENAME[100] = "logmsg.txt";

union account_t {
  long long money;
  char data[100];
};

int print_log(int n);
int single_thread();
int multi_thread();
int multi_thread_long_trx();
int scan_after_recovery();

// int main(int argc, char **argv) { return single_thread(); }
// int main(int argc, char **argv) { return print_log(20000); }
// int main(int argc, char **argv) { return multi_thread(); }
// int main(int argc, char **argv) { return multi_thread_long_trx(); }
int main(int argc, char **argv) { return scan_after_recovery(); }

int print_log(int n) {
  init_db(100, 0, 100, LOG_FILENAME, LOGMSG_FILENAME);
  descript_log_file(n);
  return 0;
}

int single_thread() {
  auto start = clock();
  char filename[TABLE_NUMBER][100];
  int64_t table_id[TABLE_NUMBER];
  init_db(5000, 0, 100, LOG_FILENAME, LOGMSG_FILENAME);
  for (int i = 0; i < TABLE_NUMBER; ++i) {
    sprintf(filename[i], "DATA%d", i + 1);
    remove(filename[i]);
    table_id[i] = file_open_table_file(filename[i]);
  }

  srand(time(NULL));

  // initialize accounts
  account_t acc;
  acc.money = INITIAL_MONEY;
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      if (db_insert(table_id[tid], rid, acc.data, sizeof(acc))) {
        LOG_ERR(-1, "failed to insert!");
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
      LOG_ERR(-1, "find failed!");
      return -1;
    }
    if (size != sizeof(acc1)) {
      LOG_ERR(-1, "invalid result!");
      return -1;
    }

    acc1.money -= money_transferred;
    if (db_update(src_table_id, src_record_id, acc1.data, sizeof(acc1), &size,
                  trx)) {
      LOG_ERR(-1, "update failed!");
      return -1;
    }
    if (size != sizeof(acc1)) {
      LOG_ERR(-1, "invalid result!");
      return -1;
    }

    if (db_find(dest_table_id, dest_record_id, acc2.data, &size, trx)) {
      LOG_ERR(-1, "find failed!");
      return -1;
    }
    if (size != sizeof(acc2)) {
      LOG_ERR(-1, "invalid result!");
      return -1;
    }

    acc2.money += money_transferred;
    if (db_update(dest_table_id, dest_record_id, acc2.data, sizeof(acc2), &size,
                  trx)) {
      LOG_ERR(-1, "update failed!");
      return -1;
    }
    if (size != sizeof(acc2)) {
      LOG_ERR(-1, "invalid result!");
      return -1;
    }

    if (rand() % 2) {
      if (trx_commit(trx) != trx) {
        LOG_ERR(-1, "commit failed!");
        return -1;
      }
    } else {
      if (trx_abort(trx) != trx) {
        LOG_ERR(-1, "abort failed!");
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
          LOG_ERR(-1, "find failed!");
          return -1;
        }
        sum_money += acc.money;
      }
    }
    if (trx_commit(trx) != trx) {
      LOG_ERR(-1, "commit failed");
      return -1;
    }
    if (sum_money != SUM_MONEY) {
      LOG_ERR(-1, "inconsistent state is detected!");
      return -1;
    }
    if ((scan + 1) % 100 == 0) LOG_INFO("%dth scan done", scan + 1);
  }
  LOG_INFO("Scan is done.");

  auto time = clock() - start;
  LOG_INFO("complete in %llf seconds", (double)time / CLOCKS_PER_SEC);

  print_debugging_infos();

  for (int i = 0; i < TABLE_NUMBER; ++i) {
    remove(filename[i]);
  }
  return 0;
}

void *transfer_thread_func(void *arg) {
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

    money_transferred = rand() % MAX_MONEY_TRANSFERRED;
    money_transferred =
        rand() % 2 == 0 ? -money_transferred : money_transferred;

    auto trx = trx_begin();
    account_t acc1, acc2;
    uint16_t size;

    if (db_find(src_table_id, src_record_id, acc1.data, &size, trx)) continue;
    if (size != sizeof(acc1)) {
      LOG_ERR(-1, "invalid result!");
      return NULL;
    }

    acc1.money -= money_transferred;
    if (db_update(src_table_id, src_record_id, acc1.data, sizeof(acc1), &size,
                  trx))
      continue;
    if (size != sizeof(acc1)) {
      LOG_ERR(-1, "invalid result!");
      return NULL;
    }

    if (db_find(dest_table_id, dest_record_id, acc2.data, &size, trx)) continue;
    if (size != sizeof(acc2)) {
      LOG_ERR(-1, "invalid result!");
      return NULL;
    }

    acc2.money += money_transferred;
    if (db_update(dest_table_id, dest_record_id, acc2.data, sizeof(acc2), &size,
                  trx))
      continue;
    if (size != sizeof(acc2)) {
      LOG_ERR(-1, "invalid result!");
      return NULL;
    }

    if (rand() % 2) {
      if (trx_commit(trx) != trx) {
        LOG_ERR(-1, "commit failed!");
        return NULL;
      }
    } else {
      if (trx_abort(trx) != trx) {
        LOG_ERR(-1, "abort failed!");
        return NULL;
      }
    }

    if ((i + 1) % 1000 == 0) LOG_INFO("%dth transfer complete", i + 1);
  }
  LOG_INFO("Transfer done.");
  return NULL;
}

void *scan_thread_func(void *arg) {
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
          return NULL;
        }
        sum_money += acc.money;
      }
      if (aborted) break;
    }
    if (!aborted) {
      if (trx_commit(trx) != trx) {
        LOG_ERR(-1, "commit failed");
        return NULL;
      }
      if (sum_money != SUM_MONEY) {
        LOG_ERR(-1, "inconsistent state is detected!");
        return NULL;
      }
    }
    if ((scan + 1) % 100 == 0) LOG_INFO("%dth scan done", scan + 1);
  }
  LOG_INFO("Scan is done.");
  return NULL;
}

int multi_thread() {
  char filename[TABLE_NUMBER][100];
  int64_t table_id[TABLE_NUMBER];
  init_db(MULTI_THREAD_BUFFER_SIZE, 0, 100, LOG_FILENAME, LOGMSG_FILENAME);
  for (int i = 0; i < TABLE_NUMBER; ++i) {
    sprintf(filename[i], "DATA%d", i + 1);
    remove(filename[i]);
    table_id[i] = file_open_table_file(filename[i]);
  }

  srand(time(NULL));

  // initialize accounts
  account_t acc;
  acc.money = INITIAL_MONEY;
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      if (db_insert(table_id[tid], rid, acc.data, sizeof(acc))) {
        LOG_ERR(-1, "failed to insert!");
        return -1;
      }
    }
  }
  LOG_INFO("initialization done");

  pthread_t transfer_threads[TRANSFER_THREAD_NUM];
  pthread_t scan_threads[SCAN_THREAD_NUM];
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

  for (int i = 0; i < TABLE_NUMBER; ++i) {
    remove(filename[i]);
  }
  return 0;
}

void *long_transfer_thread_func(void *arg) {
  int64_t table_id[TABLE_NUMBER];
  memcpy(table_id, arg, sizeof(table_id));

  int64_t src_table_id, src_record_id, dest_table_id, dest_record_id;
  long long money_transferred;

  for (int i = 0; i < TRANSFER_COUNT / TRANSFER_PER_TRX_IN_LONG_TRX; i++) {
    auto trx = trx_begin();
    int aborted = false;
    for (int j = 0; j < TRANSFER_PER_TRX_IN_LONG_TRX; ++j) {
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

      account_t acc1, acc2;
      uint16_t size;

      if (db_find(src_table_id, src_record_id, acc1.data, &size, trx)) {
        aborted = true;
        break;
      }
      if (size != sizeof(acc1)) {
        LOG_ERR(-1, "invalid result!");
        return NULL;
      }

      acc1.money -= money_transferred;
      if (db_update(src_table_id, src_record_id, acc1.data, sizeof(acc1), &size,
                    trx)) {
        aborted = true;
        break;
      }
      if (size != sizeof(acc1)) {
        LOG_ERR(-1, "invalid result!");
        return NULL;
      }

      if (db_find(dest_table_id, dest_record_id, acc2.data, &size, trx)) {
        aborted = true;
        break;
      }
      if (size != sizeof(acc2)) {
        LOG_ERR(-1, "invalid result!");
        return NULL;
      }

      acc2.money += money_transferred;
      if (db_update(dest_table_id, dest_record_id, acc2.data, sizeof(acc2),
                    &size, trx)) {
        aborted = true;
        break;
      }
      if (size != sizeof(acc2)) {
        LOG_ERR(-1, "invalid result!");
        return NULL;
      }
    }
    if (!aborted) {
      if (rand() % 2) {
        if (trx_commit(trx) != trx) {
          LOG_ERR(-1, "commit failed!");
          return NULL;
        }
      } else {
        if (trx_abort(trx) != trx) {
          LOG_ERR(-1, "abort failed!");
          return NULL;
        }
      }
    }
    if ((i + 1) % 10 == 0) LOG_INFO("%dth transfer complete", i + 1);
  }
  LOG_INFO("Transfer done.");
  return NULL;
}

int multi_thread_long_trx() {
  char filename[TABLE_NUMBER][100];
  int64_t table_id[TABLE_NUMBER];
  init_db(LONG_TRX_TEST_BUF_SIZE, 0, 100, LOG_FILENAME, LOGMSG_FILENAME);
  for (int i = 0; i < TABLE_NUMBER; ++i) {
    sprintf(filename[i], "DATA%d", i + 1);
    remove(filename[i]);
    table_id[i] = file_open_table_file(filename[i]);
  }

  srand(time(NULL));

  // initialize accounts
  account_t acc;
  acc.money = INITIAL_MONEY;
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      if (db_insert(table_id[tid], rid, acc.data, sizeof(acc))) {
        LOG_ERR(-1, "failed to insert!");
        return -1;
      }
    }
  }
  buffer_flush_all_frames();
  LOG_INFO("initialization done");

  auto start = clock();

  pthread_t transfer_threads[TRANSFER_THREAD_NUM];
  for (int i = 0; i < TRANSFER_THREAD_NUM; ++i) {
    pthread_create(&transfer_threads[i], 0, long_transfer_thread_func,
                   (void *)table_id);
  }

  for (int i = 0; i < TRANSFER_THREAD_NUM; ++i) {
    pthread_join(transfer_threads[i], NULL);
  }

  auto time = clock() - start;
  LOG_INFO("complete in %llf seconds", (double)time / CLOCKS_PER_SEC);

  for (int i = 0; i < TABLE_NUMBER; ++i) {
    remove(filename[i]);
  }

  return 0;
}

int scan_after_recovery() {
  char filename[TABLE_NUMBER][100];
  int64_t table_id[TABLE_NUMBER];
  init_db(500, 0, 100, LOG_FILENAME, LOGMSG_FILENAME);
  for (int i = 0; i < TABLE_NUMBER; ++i) {
    sprintf(filename[i], "DATA%d", i + 1);
    table_id[i] = file_open_table_file(filename[i]);
  }

  long long sum_money = 0;
  auto trx = trx_begin();
  for (int tid = 0; tid < TABLE_NUMBER; ++tid) {
    for (int rid = 0; rid < RECORD_NUMBER; ++rid) {
      account_t acc;
      uint16_t size;
      if (db_find(table_id[tid], rid, acc.data, &size, trx)) {
        LOG_ERR(-1, "find (t%d, k%lld) failed at scanning after recovery!",
                table_id[tid], rid);
        return -1;
      }
      sum_money += acc.money;
    }
  }
  if (trx_commit(trx) != trx) {
    LOG_ERR(-1, "commit failed");
    return -1;
  }
  if (sum_money != SUM_MONEY) {
    LOG_ERR(-1, "inconsistent state is detected!");
    return -1;
  }
  LOG_INFO("Scan is done.");
  return 0;
}