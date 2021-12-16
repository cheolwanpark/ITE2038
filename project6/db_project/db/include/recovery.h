#ifndef DB_RECOVERY_H_
#define DB_RECOVERY_H_

#include <stdint.h>

#include "index_manager/bpt.h"
#include "trx.h"

// types
struct log_record_t {
  uint32_t log_size;
  uint64_t lsn;
  uint64_t prev_lsn;
  trx_id_t trx_id;
  int32_t type;
  int64_t table_id;
  pagenum_t page_num;
  uint16_t offset;
  uint16_t len;
} __attribute__((packed));

// constants
constexpr int32_t BEGIN_LOG = 0;
constexpr int32_t UPDATE_LOG = 1;
constexpr int32_t COMMIT_LOG = 2;
constexpr int32_t ROLLBACK_LOG = 3;
constexpr int32_t COMPENSATE_LOG = 4;

constexpr uint64_t INITIAL_LOG_BUFFER_SIZE = 1024 * 1024;

int init_recovery(int flag, int log_num, char *log_path, char *logmsg_path);

void free_recovery();

log_record_t *create_log(trx_t *trx, int32_t type);

log_record_t *create_log_update(trx_t *trx, int64_t table_id, pagenum_t page_id,
                                uint16_t offset, uint16_t len, byte *old_img,
                                byte *new_img);

log_record_t *create_log_compensate(trx_t *trx, int64_t table_id,
                                    pagenum_t page_id, uint16_t offset,
                                    uint16_t len, byte *old_img, byte *new_img,
                                    uint64_t next_undo_seq);

byte *get_old(log_record_t *rec);
byte *get_new(log_record_t *rec);

int push_into_log_buffer(log_record_t *rec);

int flush_log();

void descript_log_file(int n);

#endif
