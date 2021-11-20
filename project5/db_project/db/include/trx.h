#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <stdint.h>

#include "disk_space_manager/file.h"

// APIs for transaction
using trx_id_t = int;
int trx_begin();
int trx_commit(trx_id_t trx_id);
int trx_abort(trx_id_t trx_id);

// APIs for locking
struct lock_t;
constexpr int S_LOCK = 0;
constexpr int X_LOCK = 1;
int init_lock_table();
int free_lock_table();
lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
                     int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

#endif
