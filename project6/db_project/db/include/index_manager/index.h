#ifndef DB_INDEX_H_
#define DB_INDEX_H_

#include <cstdint>

// Open existing data file using ‘pathname’ or create one if not existed
// return unique table id (negative on failed)
int64_t open_table(char *pathname);

// insert (key, value) with its size to data file
// return 0 on success (other value on failed);
int db_insert(int64_t table_id, int64_t key, char *value, uint16_t val_size);

// find the record with given key
// caller should allocate memory for ret_val, val_size
// return 0 on success (other value on failed)
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size,
            int trx_id);

// update the record with given key
// does not change val_size (new_val_size is used for validation)
// return 0 on success (other value on failed)
int db_update(int64_t table_id, int64_t key, char *values,
              uint16_t new_val_size, uint16_t *old_val_size, int trx_id);

// find the matching record and delete it if found
// return 0 on success (other value on failed)
int db_delete(int64_t table_id, int64_t key);

#endif