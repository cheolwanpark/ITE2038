#ifndef DB_TRX_H_
#define DB_TRX_H_

#include <stdint.h>

#include "disk_space_manager/file.h"

// constants
constexpr double DEADLOCK_CHECK_RUNTIME_THRESHOLD = 5.0;
constexpr double DEADLOCK_CHECK_INTERVAL = 5.0;
constexpr int S_LOCK = 0;
constexpr int X_LOCK = 1;

// types
using trx_id_t = int;
struct lock_t;
struct trx_t;

// APIs for transaction
int trx_begin();
int trx_commit(trx_id_t trx_id);
int trx_abort(trx_id_t trx_id);
int trx_log_update(trx_t* trx, int64_t table_id, pagenum_t page_id,
                   uint16_t offset, uint16_t len, const byte* bef);

// APIs for locking
int init_lock_table();
int free_lock_table();

int convert_implicit_lock(int table_id, pagenum_t page_id, int64_t key,
                          trx_id_t trx_id, int* slotnum);
trx_t* try_implicit_lock(int64_t table_id, pagenum_t page_id, int64_t key,
                         trx_id_t trx_id, int slotnum);

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
                     int trx_id, int lock_mode);
trx_t* lock_acquire_compression(int64_t table_id, pagenum_t page_id,
                                int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);
trx_t* get_trx(lock_t* lock);

void print_debugging_infos();

#endif
