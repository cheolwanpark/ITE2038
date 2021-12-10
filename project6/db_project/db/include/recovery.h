#ifndef DB_RECOVERY_H_
#define DB_RECOVERY_H_

#include <stdint.h>

#include "trx.h"

// constants
constexpr int32_t BEGIN_LOG = 0;
constexpr int32_t UPDATE_LOG = 1;
constexpr int32_t COMMIT_LOG = 2;
constexpr int32_t ROLLBACK_LOG = 3;
constexpr int32_t COMPENSATE_LOG = 4;

int init_recovery(int flag, int log_num, char *log_path, char *logmsg_path);

void free_recovery();

int log(trx_t *trx, int32_t type);

int log_update(trx_t *trx, int64_t table_id, pagenum_t page_id, uint16_t offset,
               uint16_t len, byte *old_img, byte *new_img);

int log_compensate(trx_t *trx, int64_t table_id, pagenum_t page_id,
                   uint16_t offset, uint16_t len, byte *old_img, byte *new_img);

#endif
