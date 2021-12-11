#include "recovery.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "log.h"

struct log_list_t {
  log_list_t *next;
  log_record_t *rec;
};

uint64_t LSN = 1;
pthread_mutex_t log_latch = PTHREAD_MUTEX_INITIALIZER;

log_list_t *log_head = NULL, *log_tail = NULL;
int log_fd = -1, logmsg_fd = -1;

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
  if (access(log_path, F_OK) != 0) {
    log_fd = open(log_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (log_fd < 0) {
      LOG_ERR("failed to create and open log file %s, errno: %s", log_path,
              strerror(errno));
      return 1;
    }

    // write guard log_size (for reading)
    uint32_t guard = 0;
    if (write(log_fd, &guard, sizeof(guard)) != sizeof(guard)) {
      LOG_ERR("cannot write log, errno: %s", strerror(errno));
      return 1;
    }
    if (fsync(log_fd) < 0) {
      LOG_ERR("cannot sync log file, errno: %s", strerror(errno));
      return 1;
    }

  } else {
    log_fd = open(log_path, O_RDWR);
    if (log_fd < 0) {
      LOG_ERR("failed to open log file %s, errno: %s", log_path,
              strerror(errno));
      return 1;
    }
    if (lseek(log_fd, -sizeof(uint32_t), SEEK_END) <
        0) {  // write logs beyond the end point (-4 is ignoring guard logsize)
      LOG_ERR("failed to lseek, errno: %s", strerror(errno));
      return 1;
    }
  }
  return 0;
}

void free_recovery() {
  // flush all logs
  flush_log();
  if (log_fd >= 0) close(log_fd);
  if (logmsg_fd >= 0) close(logmsg_fd);

  pthread_mutex_destroy(&log_latch);
}

log_record_t *create_log(trx_t *trx, int32_t type) {
  if (trx == NULL ||
      (type != BEGIN_LOG && type != COMMIT_LOG && type != ROLLBACK_LOG)) {
    LOG_ERR("invalid parameters");
    return NULL;
  }
  log_record_t *rec = (log_record_t *)malloc(sizeof(log_record_t));
  if (rec == NULL) {
    LOG_ERR("failed to allocate");
    return NULL;
  }
  rec->log_size = 28;
  rec->lsn = LSN++;
  rec->prev_lsn = trx->last_lsn;
  rec->trx_id = trx->id;
  rec->type = type;

  trx->last_lsn = rec->lsn;

  return rec;
}

log_record_t *create_log_update(trx_t *trx, bpt_page_t *page, int64_t table_id,
                                pagenum_t page_id, uint16_t offset,
                                uint16_t len, byte *old_img, byte *new_img) {
  if (trx == NULL || old_img == NULL || new_img == NULL) {
    LOG_ERR("invalid parameters");
    return NULL;
  }

  log_record_t *rec = (log_record_t *)malloc(sizeof(log_record_t) + 2 * len);
  if (rec == NULL) {
    LOG_ERR("failed to allocate");
    return NULL;
  }
  auto lsn = LSN++;

  page->header.page_lsn = lsn;
  set_dirty(page);

  rec->log_size = sizeof(log_record_t) + 2 * len;
  rec->lsn = lsn;
  rec->prev_lsn = trx->last_lsn;
  rec->trx_id = trx->id;
  rec->type = UPDATE_LOG;
  rec->table_id = table_id;
  rec->page_num = page_id;
  rec->offset = offset;
  rec->len = len;
  set_images(rec, len, old_img, new_img);

  trx->last_lsn = rec->lsn;

  return rec;
}

log_record_t *create_log_compensate(trx_t *trx, bpt_page_t *page,
                                    int64_t table_id, pagenum_t page_id,
                                    uint16_t offset, uint16_t len,
                                    byte *old_img, byte *new_img,
                                    uint64_t next_undo_seq) {
  if (trx == NULL || old_img == NULL || new_img == NULL) {
    LOG_ERR("invalid parameters");
    return NULL;
  }
  auto lsn = LSN++;

  page->header.page_lsn = lsn;
  set_dirty(page);

  log_record_t *rec =
      (log_record_t *)malloc(sizeof(log_record_t) + 2 * len + 8);
  if (rec == NULL) {
    LOG_ERR("failed to allocate");
    return NULL;
  }
  rec->log_size = sizeof(log_record_t) + 2 * len + 8;
  rec->lsn = lsn;
  rec->prev_lsn = trx->last_lsn;
  rec->trx_id = trx->id;
  rec->type = COMPENSATE_LOG;
  rec->table_id = table_id;
  rec->page_num = page_id;
  rec->offset = offset;
  rec->len = len;
  set_images(rec, len, old_img, new_img);
  set_next_undo_lsn(rec, next_undo_seq);

  trx->last_lsn = rec->lsn;

  return rec;
}

int push_into_log_buffer(log_record_t *rec) {
  if (rec == NULL) return 1;

  log_list_t *new_node = (log_list_t *)malloc(sizeof(log_list_t));
  if (new_node == NULL) {
    LOG_ERR("failed to allocate");
    return 1;
  }
  new_node->next = NULL;
  new_node->rec = rec;

  pthread_mutex_lock(&log_latch);
  if (log_tail == NULL) {  // no log exists
    log_head = log_tail = new_node;
  } else {
    log_tail->next = new_node;
    log_tail = new_node;
  }
  pthread_mutex_unlock(&log_latch);

  return 0;
}

int flush_log() {
  pthread_mutex_lock(&log_latch);

  if (lseek(log_fd, -sizeof(uint32_t), SEEK_END) <
      0) {  // go to flush start position
    LOG_ERR("failed to seek on log, errno: %s", strerror(errno));
    return 1;
  }

  // LOG_INFO("flushing!");
  while (log_head != NULL) {
    auto *current = log_head;
    log_head = log_head->next;

    auto *rec = current->rec;
    if (write(log_fd, rec, rec->log_size) != rec->log_size) {
      LOG_ERR("cannot write log, errno: %s", strerror(errno));
      return 1;
    }
    // LOG_INFO("writed %d", rec->len);
    if (rec->type != UPDATE_LOG && rec->type != COMPENSATE_LOG) free(rec);
    free(current);
  }
  log_head = log_tail = NULL;

  // write guard log_size (for reading)
  uint32_t guard = 0;
  if (write(log_fd, &guard, sizeof(guard)) != sizeof(guard)) {
    LOG_ERR("cannot write log, errno: %s", strerror(errno));
    return 1;
  }

  if (fsync(log_fd) < 0) {
    LOG_ERR("cannot sync log file, errno: %s", strerror(errno));
    return 1;
  }
  pthread_mutex_unlock(&log_latch);
  return 0;
}

void descript_log_file(int n) {
  uint32_t log_size;
  log_record_t *rec = NULL;

  if (lseek(log_fd, 0, SEEK_SET) < 0) {
    LOG_ERR("failed to seek");
    return;
  }

  while (n-- && read(log_fd, &log_size, 4) == 4 && log_size != 0) {
    if (lseek(log_fd, -4, SEEK_CUR) < 0) {
      LOG_ERR("failed to seek");
      return;
    }
    rec = (log_record_t *)malloc(log_size);
    if (rec == NULL) {
      LOG_ERR("failed to allocate log record struct");
      return;
    }
    auto read_res = read(log_fd, rec, log_size);
    if (read_res < 0) {
      LOG_ERR("failed to read, %s", strerror(errno));
      return;
    } else if (read_res == 0)
      break;

    switch (rec->type) {
      case BEGIN_LOG:
        printf("%llu: trx %d beg : prev %llu\n", rec->lsn, rec->trx_id,
               rec->prev_lsn);
        break;
      case COMMIT_LOG:
        printf("%llu: trx %d commit : prev %llu\n", rec->lsn, rec->trx_id,
               rec->prev_lsn);
        break;
      case ROLLBACK_LOG:
        printf("%llu: trx %d rollback : prev %llu\n", rec->lsn, rec->trx_id,
               rec->prev_lsn);
        break;
      case UPDATE_LOG:
        printf("%llu: update on trx %d (rec(%lld, %llu, %u)) : prev %llu\n",
               rec->lsn, rec->trx_id, rec->table_id, rec->page_num, rec->offset,
               rec->prev_lsn);
        break;
      case COMPENSATE_LOG:
        printf(
            "%llu: compensate on trx %d (rec(%lld, %llu, %u)) : prev %llu, "
            "next undo: %llu\n",
            rec->lsn, rec->trx_id, rec->table_id, rec->page_num, rec->offset,
            rec->prev_lsn, get_next_undo_lsn(rec));
        break;
    }

    free(rec);
  }
}