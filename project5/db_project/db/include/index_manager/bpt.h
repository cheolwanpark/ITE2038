#ifndef DB_BPT_PAGE_H_
#define DB_BPT_PAGE_H_

#include "buffer_manager.h"

// constants
constexpr uint64_t kBptPageHeaderSize = 128;

// type definitions
typedef int64_t bpt_key_t;

struct bpt_header_t {  // used in lock manager
  pagenum_t parent_page;
  uint32_t is_leaf;
  uint32_t num_of_keys;
};

union bpt_page_t {
  page_t page;
  bpt_header_t header;
};

// find record
// if trx_id is less than 1, then do nothing with trx
// return true on success
bool bpt_find(int64_t table_id, pagenum_t root, bpt_key_t key, uint16_t *size,
              byte *value, int trx_id);

// update record
// if trx_id is less than 1, then do nothing with trx
// return true on success
bool bpt_update(int64_t table_id, pagenum_t root, bpt_key_t key, byte *value,
                uint16_t new_val_size, uint16_t *old_val_size, int trx_id);

// insert new record
// return root (0 on failed)
pagenum_t bpt_insert(int64_t table_id, pagenum_t root, bpt_key_t key,
                     uint16_t size, const byte *value);

// delete record
// return root (0 on failed)
pagenum_t bpt_delete(int64_t table_id, pagenum_t root, bpt_key_t key);

#include <limits.h>
bool is_clean(int64_t table_id, pagenum_t root, pagenum_t parent = 0,
              bpt_key_t min = INT_MIN, bpt_key_t max = INT_MAX,
              bool is_root = true, bool is_first_child = false);

#endif