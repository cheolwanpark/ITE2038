#ifndef DB_BPT_PAGE_H_
#define DB_BPT_PAGE_H_

#include "disk_space_manager/file.h"

// this functions should be used in index_manager/index.h and test only

// key type
typedef int64_t bpt_key_t;

// find record
// return true on success
bool bpt_find(int64_t table_id, pagenum_t root, bpt_key_t key, uint16_t *size,
              byte *value);

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
              bool is_root = true);

#endif