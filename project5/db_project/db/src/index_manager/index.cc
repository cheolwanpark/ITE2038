#include "index_manager/index.h"

#include <cstddef>

#include "buffer_manager.h"
#include "index_manager/bpt.h"
#include "log.h"

int64_t open_table(char *pathname) { return file_open_table_file(pathname); }

int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  auto *header = buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  auto root = header->header.root_page_number;
  unpin(table_id, kHeaderPagenum);
  root = bpt_insert(table_id, root, key, val_size, value);
  if (root == 0) return 1;
  header = buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  header->header.root_page_number = root != kNullPagenum ? root : 0;
  set_dirty((page_t *)header);
  unpin((page_t *)header);

  return 0;
}

int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size,
            int trx_id) {
  if (table_id < 0 || ret_val == NULL || val_size == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  auto *header = buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  auto root = header->header.root_page_number;
  unpin((page_t *)header);
  if (bpt_find(table_id, root, key, val_size, ret_val, trx_id))
    return 0;
  else
    return 1;
}

int db_update(int64_t table_id, int64_t key, char *values,
              uint16_t new_val_size, uint16_t *old_val_size, int trx_id) {
  if (table_id < 0 || values == NULL || old_val_size == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  auto *header = buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  auto root = header->header.root_page_number;
  unpin((page_t *)header);
  if (bpt_update(table_id, root, key, values, new_val_size, old_val_size,
                 trx_id))
    return 0;
  else
    return 1;
}

int db_delete(int64_t table_id, int64_t key) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  auto *header = buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  auto root = header->header.root_page_number;
  unpin((page_t *)header);
  root = bpt_delete(table_id, root, key);
  if (root == 0) return 1;
  header = buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  header->header.root_page_number = root != kNullPagenum ? root : 0;
  set_dirty((page_t *)header);
  unpin((page_t *)header);

  return 0;
}