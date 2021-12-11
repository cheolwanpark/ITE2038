#ifndef DB_BPT_PAGE_H_
#define DB_BPT_PAGE_H_

#include <stdint.h>
#include <stdlib.h>

#include "buffer_manager.h"

// constants
constexpr uint64_t kBptPageHeaderSize = 128;

// type definitions
typedef int64_t bpt_key_t;
struct lock_t;

struct bpt_header_t {  // used in lock manager
  pagenum_t parent_page;
  uint32_t is_leaf;
  uint32_t num_of_keys;
  uint64_t __reserved;
  uint64_t page_lsn;
};

union bpt_page_t {
  page_t page;
  bpt_header_t header;
};

// find record
// if trx_id is less than 1, then do nothing with trx
// return true on success
bool bpt_find(int64_t table_id, pagenum_t root, bpt_key_t key, uint16_t *size,
              byte *value, int trx_id, lock_t *lock = NULL);

// update record
// if trx_id is less than 1, then do nothing with trx
// return true on success
bool bpt_update(int64_t table_id, pagenum_t root, bpt_key_t key, byte *value,
                uint16_t new_val_size, uint16_t *old_val_size, int trx_id,
                lock_t *lock = NULL);

// insert new record
// return root (0 on failed)
pagenum_t bpt_insert(int64_t table_id, pagenum_t root, bpt_key_t key,
                     uint16_t size, const byte *value);

// delete record
// return root (0 on failed)
pagenum_t bpt_delete(int64_t table_id, pagenum_t root, bpt_key_t key);

#endif