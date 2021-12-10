#include "recovery.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "log.h"

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
};

struct log_list_t {
  log_list_t *next;
  log_record_t *rec;
};

uint64_t LSN = 1;
pthread_mutex_t log_latch = PTHREAD_MUTEX_INITIALIZER;

log_list_t *log_head = NULL, *log_tail = NULL;

int insert_into_log_list(log_record_t *rec) {
  if (rec == NULL) return 1;

  log_list_t *new_node = (log_list_t *)malloc(sizeof(log_list_t));
  if (new_node == NULL) {
    LOG_ERR("failed to allocate");
    return 1;
  }
  new_node->next = NULL;
  new_node->rec = rec;

  if (log_tail == NULL) {  // no log exists
    log_head = log_tail = new_node;
  } else {
    log_tail->next = new_node;
    log_tail = new_node;
  }

  return 0;
}

byte *get_old(log_record_t *rec) {
  if (rec == NULL) return NULL;
  return ((byte *)rec) + 48;
}

byte *get_new(log_record_t *rec) {
  if (rec == NULL) return NULL;
  return ((byte *)rec) + (48 + rec->len);
}

void set_images(log_record_t *rec, uint16_t len, byte *old_img, byte *new_img) {
  if (rec == NULL || old_img == NULL || new_img == NULL) return;
  rec->len = len;
  memcpy(((byte *)rec) + 48, old_img, len);
  memcpy(((byte *)rec) + (48 + len), new_img, len);
}

uint64_t get_next_undo_lsn(log_record_t *rec) {
  if (rec == NULL || rec->type != COMPENSATE_LOG) return 0;
  uint64_t *v = (uint64_t *)(((byte *)rec) + (rec->log_size - 8));
  return *v;
}

int set_next_undo_lsn(log_record_t *rec, uint64_t val) {
  if (rec == NULL || rec->type != COMPENSATE_LOG) return 1;
  uint64_t *v = (uint64_t *)(((byte *)rec) + (rec->log_size - 8));
  *v = val;
  return 0;
}

// API functions
int init_recovery(int flag, int log_num, char *log_path, char *logmsg_path) {
  return 0;
}

void free_recovery() {
  // flush all logs
  pthread_mutex_lock(&log_latch);
  auto *iter = log_head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->next;
    // flush
    free(current->rec);
    free(current);
  }
  pthread_mutex_unlock(&log_latch);

  pthread_mutex_destroy(&log_latch);
}

int log(trx_t *trx, int32_t type) {
  if (trx == NULL ||
      (type != BEGIN_LOG && type != COMMIT_LOG && type != ROLLBACK_LOG)) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  log_record_t *rec = (log_record_t *)malloc(sizeof(log_record_t));
  if (rec == NULL) {
    LOG_ERR("failed to allocate");
    return 1;
  }
  rec->log_size = 28;
  rec->lsn = LSN++;
  rec->prev_lsn = trx->last_lsn;
  rec->trx_id = trx->id;
  rec->type = type;

  pthread_mutex_lock(&log_latch);
  if (insert_into_log_list(rec)) {
    pthread_mutex_unlock(&log_latch);
    free(rec);
    LOG_ERR("failed to insert into log list(buffer)");
    return 1;
  }
  pthread_mutex_unlock(&log_latch);

  return 0;
}

int log_update(trx_t *trx, int64_t table_id, pagenum_t page_id, uint16_t offset,
               uint16_t len, byte *old_img, byte *new_img) {
  if (trx == NULL || old_img == NULL || new_img == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }

  log_record_t *rec = (log_record_t *)malloc(sizeof(log_record_t));
  if (rec == NULL) {
    LOG_ERR("failed to allocate");
    return 1;
  }
  rec->log_size = 28;
  rec->lsn = LSN++;
  rec->prev_lsn = trx->last_lsn;
  rec->trx_id = trx->id;
  rec->type = UPDATE_LOG;
  rec->table_id = table_id;
  rec->page_num = page_id;
  rec->offset = offset;
  rec->len = len;
  set_images(rec, len, old_img, new_img);

  pthread_mutex_lock(&log_latch);
  if (insert_into_log_list(rec)) {
    pthread_mutex_unlock(&log_latch);
    free(rec);
    LOG_ERR("failed to insert into log list(buffer)");
    return 1;
  }
  pthread_mutex_unlock(&log_latch);
  return 0;
}

int log_compensate(trx_t *trx, int64_t table_id, pagenum_t page_id,
                   uint16_t offset, uint16_t len, byte *old_img, byte *new_img,
                   log_record_t *upd_log) {
  if (trx == NULL || old_img == NULL || new_img == NULL || upd_log == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }

  log_record_t *rec = (log_record_t *)malloc(sizeof(log_record_t));
  if (rec == NULL) {
    LOG_ERR("failed to allocate");
    return 1;
  }
  rec->log_size = 28;
  rec->lsn = LSN++;
  rec->prev_lsn = trx->last_lsn;
  rec->trx_id = trx->id;
  rec->type = COMPENSATE_LOG;
  rec->table_id = table_id;
  rec->page_num = page_id;
  rec->offset = offset;
  rec->len = len;
  set_images(rec, len, old_img, new_img);
  set_next_undo_lsn(rec, upd_log->prev_lsn);

  pthread_mutex_lock(&log_latch);
  if (insert_into_log_list(rec)) {
    pthread_mutex_unlock(&log_latch);
    free(rec);
    LOG_ERR("failed to insert into log list(buffer)");
    return 1;
  }
  pthread_mutex_unlock(&log_latch);
  return 0;
}