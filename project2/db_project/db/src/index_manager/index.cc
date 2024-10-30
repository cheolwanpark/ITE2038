#include "index_manager/index.h"

#include <cstddef>

#include "disk_space_manager/file.h"
#include "index_manager/bpt.h"
#include "log.h"

int64_t open_table(char *pathname) { return file_open_table_file(pathname); }

int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  header_page_t header;
  file_read_header_page(table_id, &header);

  auto root = header.header.root_page_number;
  root = bpt_insert(table_id, root, key, val_size, value);
  if (root == 0) return 1;
  file_read_header_page(table_id, &header);
  header.header.root_page_number = root != kNullPagenum ? root : 0;
  file_write_header_page(table_id, &header);

  return 0;
}

int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size) {
  if (table_id < 0 || ret_val == NULL || val_size == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  header_page_t header;
  file_read_header_page(table_id, &header);

  auto root = header.header.root_page_number;
  if (bpt_find(table_id, root, key, val_size, ret_val))
    return 0;
  else
    return 1;
}

int db_delete(int64_t table_id, int64_t key) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  header_page_t header;
  file_read_header_page(table_id, &header);

  auto root = header.header.root_page_number;
  root = bpt_delete(table_id, root, key);
  if (root == 0) return 1;
  file_read_header_page(table_id, &header);
  header.header.root_page_number = root != kNullPagenum ? root : 0;
  file_write_header_page(table_id, &header);

  return 0;
}

bool is_clean(int64_t table_id) {
  header_page_t header;
  file_read_header_page(table_id, &header);
  auto root = header.header.root_page_number;
  return is_clean(table_id, root);
}