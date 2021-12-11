#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <stdint.h>
#include <time.h>

#include "disk_space_manager/file.h"
#include "index_manager/bpt.h"

// constants
constexpr double DEADLOCK_CHECK_RUNTIME_THRESHOLD = 5.0;
constexpr double DEADLOCK_CHECK_INTERVAL = 5.0;
constexpr int S_LOCK = 0;
constexpr int X_LOCK = 1;

// types
using trx_id_t = int;

struct lock_t;
struct update_log_t;
struct log_record_t;
struct trx_t {
  trx_id_t id;
  clock_t start_time;
  lock_t* head;
  lock_t* dummy_head;
  update_log_t* log_head;
  int releasing;
  uint64_t last_lsn;
};

// APIs for transaction
int trx_begin();
int trx_commit(trx_id_t trx_id);
int trx_abort(trx_id_t trx_id);
int trx_log_update(trx_t* trx, log_record_t* rec);

// APIs for locking
int init_lock_table();
int free_lock_table();

lock_t* lock_acquire(bpt_page_t** page_ptr, int64_t table_id, pagenum_t page_id,
                     int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);
trx_t* get_trx(lock_t* lock);

void print_debugging_infos();

#endif
