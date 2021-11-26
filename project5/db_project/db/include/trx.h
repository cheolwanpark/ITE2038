#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <stdint.h>

#include "disk_space_manager/file.h"

// constants
constexpr double DEADLOCK_CHECK_RUNTIME_THRESHOLD = 5.0;
constexpr double DEADLOCK_CHECK_INTERVAL = 5.0;
constexpr int S_LOCK = 0;
constexpr int X_LOCK = 1;

// APIs for transaction
using trx_id_t = int;
int trx_begin();
int trx_commit(trx_id_t trx_id);
int trx_abort(trx_id_t trx_id);
int trx_log_update(trx_id_t trx_id, int64_t table_id, pagenum_t page_id,
                   uint16_t offset, uint16_t len, const byte* bef);

// APIs for locking
struct lock_t;
int init_lock_table();
int free_lock_table();
lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
                     int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

#endif
