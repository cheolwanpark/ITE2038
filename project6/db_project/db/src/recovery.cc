#include "recovery.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <map>
#include <set>

#include "buffer_manager.h"
#include "log.h"
#include "trx.h"

struct log_list_t {
  log_list_t *next;
  log_record_t *rec;
};

uint64_t LSN = 1;
pthread_mutex_t log_latch = PTHREAD_RECURSIVE_MUTEX_INITIALIZER;

int log_fd = -1;
FILE *logmsg_fp = NULL;
byte *log_buffer = NULL;
uint64_t log_buffer_size = 0;
uint64_t log_buffer_max_size = INITIAL_LOG_BUFFER_SIZE;

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

int analysis_phase(std::set<trx_id_t> &winners, std::set<trx_id_t> &losers) {
  uint32_t log_size;
  uint32_t current_position = 0;
  uint64_t current_lsn = 0;
  trx_id_t max_trx_id = 0;

  // allocate maximum size log_record (max slot length is 108)
  log_record_t *rec =
      (log_record_t *)malloc(sizeof(log_record_t) + 108 * 2 + 8);
  if (rec == NULL) {
    LOG_ERR("failed to allocate record struct, %s", strerror(errno));
    return 1;
  }

  if (fprintf(logmsg_fp, "[ANALYSIS] Analysis pass start\n") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }

  while (true) {
    if (lseek(log_fd, current_position, SEEK_SET) < 0) {
      LOG_ERR("failed to seek, %s", strerror(errno));
      return 1;
    }
    if (read(log_fd, &log_size, sizeof(log_size)) != sizeof(log_size) ||
        log_size == 0)
      break;
    if (lseek(log_fd, -4, SEEK_CUR) < 0) {
      LOG_ERR("failed to seek, %s", strerror(errno));
      return 1;
    }

    if (read(log_fd, rec, log_size) != log_size) break;

    current_lsn = rec->lsn;
    switch (rec->type) {
      case BEGIN_LOG:
        if (!losers.insert(rec->trx_id).second) {
          LOG_ERR("%d already exists in losers", rec->trx_id);
          return 1;
        }
        break;

      case COMMIT_LOG:
      case ROLLBACK_LOG:
        if (losers.erase(rec->trx_id) == 0) {
          LOG_ERR("%d is not in the losers", rec->trx_id);
          return 1;
        }
        if (!winners.insert(rec->trx_id).second) {
          LOG_ERR("%d already exists in winners", rec->trx_id);
          return 1;
        }
        break;
    }
    current_position += log_size;
  }
  free(rec);

  LSN = current_lsn + 1;

  if (fprintf(logmsg_fp, "[ANALYSIS] Analysis success. Winner:") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }
  for (auto id : winners) {
    if (fprintf(logmsg_fp, " %d", id) < 0) {
      LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
      return 1;
    }
    max_trx_id = std::max(id, max_trx_id);
  }
  if (fprintf(logmsg_fp, ", Loser:") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }
  for (auto id : losers) {
    if (fprintf(logmsg_fp, " %d", id) < 0) {
      LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
      return 1;
    }
    max_trx_id = std::max(id, max_trx_id);
  }
  if (fprintf(logmsg_fp, "\n") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }
  if (fflush(logmsg_fp) != 0) {
    LOG_ERR("failed to flush logmsg file, %s", strerror(errno));
    return 1;
  }

  set_trx_counter(max_trx_id + 1);

  return 0;
}

int redo_phase(std::set<trx_id_t> &winners, std::set<trx_id_t> &losers,
               std::map<uint64_t, uint64_t> &lsn_position_map) {
  uint32_t log_size;
  uint32_t current_position = 0;

  // add losers as a active trx
  for (auto id : losers) {
    if (add_active_trx(id)) {
      LOG_ERR("failed to add %d as a active trx", id);
      return 1;
    }
  }

  // allocate maximum size log_record (max slot length is 108)
  log_record_t *rec =
      (log_record_t *)malloc(sizeof(log_record_t) + 108 * 2 + 8);
  if (rec == NULL) {
    LOG_ERR("failed to allocate record struct, %s", strerror(errno));
    return 1;
  }

  if (fprintf(logmsg_fp, "[REDO] Redo pass start\n") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }

  while (true) {
    if (lseek(log_fd, current_position, SEEK_SET) < 0) {
      LOG_ERR("failed to seek, %s", strerror(errno));
      return 1;
    }
    if (read(log_fd, &log_size, sizeof(log_size)) != sizeof(log_size) ||
        log_size == 0)
      break;
    if (lseek(log_fd, -4, SEEK_CUR) < 0) {
      LOG_ERR("failed to seek, %s", strerror(errno));
      return 1;
    }

    if (read(log_fd, rec, log_size) != log_size) break;

    auto is_loser = losers.find(rec->trx_id) != losers.end();

    switch (rec->type) {
      case BEGIN_LOG:
        if (fprintf(logmsg_fp, "LSN %llu [BEGIN] Transaction id %d\n", rec->lsn,
                    rec->trx_id) < 0) {
          LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
          return 1;
        }
        break;

      case COMMIT_LOG:
        if (fprintf(logmsg_fp, "LSN %llu [COMMIT] Transaction id %d\n",
                    rec->lsn, rec->trx_id) < 0) {
          LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
          return 1;
        }
        break;

      case ROLLBACK_LOG:
        if (fprintf(logmsg_fp, "LSN %llu [ROLLBACK] Transaction id %d\n",
                    rec->lsn, rec->trx_id) < 0) {
          LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
          return 1;
        }
        break;

      case UPDATE_LOG:
      case COMPENSATE_LOG:
        if (file_open_table_file(rec->table_id) < 0) {
          LOG_ERR("failed to open table file");
          return 1;
        }
        auto *page =
            buffer_get_page_ptr<bpt_page_t>(rec->table_id, rec->page_num);

        if (page->header.page_lsn < rec->lsn) {
          memcpy(page->page.data + rec->offset, get_new(rec), rec->len);
          page->header.page_lsn = rec->lsn;
          set_dirty(page);

          if (rec->type == UPDATE_LOG) {
            if (fprintf(logmsg_fp,
                        "LSN %llu [UPDATE] Transaction id %d redo apply\n",
                        rec->lsn, rec->trx_id) < 0) {
              unpin(page);
              LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
              return 1;
            }
          } else {
            if (fprintf(logmsg_fp, "LSN %llu [CLR] next undo lsn %llu\n",
                        rec->lsn, get_next_undo_lsn(rec)) < 0) {
              unpin(page);
              LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
              return 1;
            }
          }
        } else {
          if (fprintf(logmsg_fp, "LSN %llu [CONSIDER-REDO] Transaction id %d\n",
                      rec->lsn, rec->trx_id) < 0) {
            unpin(page);
            LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
            return 1;
          }
        }

        unpin(page);
        break;
    }

    if (is_loser) {
      lsn_position_map[rec->lsn] = current_position;
      auto *trx = get_trx(rec->trx_id);
      if (trx == NULL) {
        LOG_ERR("loser trx %d is not in the active trx table");
        return 1;
      }
      trx->last_lsn = rec->lsn;
    }

    current_position += log_size;
  }
  free(rec);

  if (fprintf(logmsg_fp, "[REDO] Redo pass end\n") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }
  if (fflush(logmsg_fp) != 0) {
    LOG_ERR("failed to flush logmsg file, %s", strerror(errno));
    return 1;
  }

  return 0;
}

int undo_phase(std::set<trx_id_t> &winners, std::set<trx_id_t> &losers,
               std::map<uint64_t, uint64_t> &lsn_pos_map) {
  uint32_t log_size;

  // allocate maximum size log_record (max slot length is 108)
  log_record_t *rec =
      (log_record_t *)malloc(sizeof(log_record_t) + 108 * 2 + 8);
  if (rec == NULL) {
    LOG_ERR("failed to allocate record struct, %s", strerror(errno));
    return 1;
  }

  if (fprintf(logmsg_fp, "[UNDO] Undo pass start\n") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }

  std::map<trx_id_t, uint64_t> next_undo_lsn_map;
  for (auto id : losers) {
    next_undo_lsn_map[id] = UINT64_MAX;
  }

  for (auto it = lsn_pos_map.rbegin(); it != lsn_pos_map.rend(); it++) {
    auto lsn = it->first;
    auto position = it->second;

    if (lseek(log_fd, position, SEEK_SET) < 0) {
      LOG_ERR("failed to seek, %s", strerror(errno));
      return 1;
    }
    if (read(log_fd, &log_size, sizeof(log_size)) != sizeof(log_size) ||
        log_size == 0)
      break;
    if (lseek(log_fd, -4, SEEK_CUR) < 0) {
      LOG_ERR("failed to seek, %s", strerror(errno));
      return 1;
    }
    if (read(log_fd, rec, log_size) != log_size) break;

    if (rec->type == BEGIN_LOG) {
      auto *trx = get_trx(rec->trx_id);
      if (trx == NULL) {
        LOG_ERR("failed to get loser trx %d", rec->trx_id);
        return 1;
      }
      auto *rec = create_log(trx, ROLLBACK_LOG);
      if (rec == NULL) {
        LOG_ERR("failed to create log");
        return 0;
      }
      if (push_into_log_buffer(rec)) {
        free(rec);
        LOG_ERR("failed to push into log buffer");
        return 0;
      }
      free(rec);

      losers.erase(rec->trx_id);
      if (remove_active_trx(rec->trx_id)) {
        LOG_ERR("failed to remove from active trx table");
        return 1;
      }
    }

    if (next_undo_lsn_map[rec->trx_id] < rec->lsn)
      continue;
    else if (rec->type == COMPENSATE_LOG) {
      next_undo_lsn_map[rec->trx_id] = get_next_undo_lsn(rec);
    } else if (rec->type == UPDATE_LOG) {
      auto *trx = get_trx(rec->trx_id);
      if (trx == NULL) {
        LOG_ERR("failed to get loser trx %d", rec->trx_id);
        return 1;
      }
      auto *new_rec = create_log_compensate(trx, rec->table_id, rec->page_num,
                                            rec->offset, rec->len, get_new(rec),
                                            get_old(rec), rec->prev_lsn);
      if (new_rec == NULL) {
        LOG_ERR("failed to create new compensate log");
        return 0;
      }

      auto *page =
          buffer_get_page_ptr<bpt_page_t>(rec->table_id, rec->page_num);
      memcpy(page->page.data + rec->offset, get_old(rec), rec->len);
      set_dirty(page);
      if (push_into_log_buffer(new_rec)) {
        free(new_rec);
        LOG_ERR("failed to push log into log buffer");
        return 0;
      }
      page->header.page_lsn = new_rec->lsn;
      set_dirty(page);
      unpin(page);
      free(new_rec);

      if (fprintf(logmsg_fp, "LSN %llu [UPDATE] Transaction id %d undo apply\n",
                  rec->lsn, rec->trx_id) < 0) {
        LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
        return 1;
      }
    }
  }
  free(rec);

  if (fprintf(logmsg_fp, "[UNDO] Undo pass end\n") < 0) {
    LOG_ERR("failed to write into logmsg file, %s", strerror(errno));
    return 1;
  }
  if (fflush(logmsg_fp) != 0) {
    LOG_ERR("failed to flush logmsg file, %s", strerror(errno));
    return 1;
  }

  return 0;
}

int recovery_process() {
  std::set<trx_id_t> winners, losers;
  std::map<uint64_t, uint64_t> lsn_position_map;
  pthread_mutex_lock(&log_latch);
  if (analysis_phase(winners, losers)) {
    LOG_ERR("failed to perform an alysis!");
    return 1;
  }
  if (redo_phase(winners, losers, lsn_position_map)) {
    LOG_ERR("failed to perform redo!");
    return 1;
  }
  if (undo_phase(winners, losers, lsn_position_map)) {
    LOG_ERR("failed to perform undo!");
    return 1;
  }
  pthread_mutex_unlock(&log_latch);
  return 0;
}

// API functions
int init_recovery(int flag, int log_num, char *log_path, char *logmsg_path) {
  // allocate log buffer
  log_buffer = (byte *)malloc(log_buffer_max_size);
  if (log_buffer == NULL) {
    LOG_ERR("failed to allocate log buffer");
    return 1;
  }

  // open logmsg file
  logmsg_fp = fopen(logmsg_path, "a+");
  if (logmsg_fp == NULL) {
    LOG_ERR("failed to open or create %s, errno: %s", logmsg_path,
            strerror(errno));
    return 1;
  }

  // open log file
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

    if (recovery_process()) {
      LOG_ERR("failed to recovery!");
      return 1;
    }
  }
  return 0;
}

void free_recovery() {
  // flush all logs
  flush_log();
  pthread_mutex_lock(&log_latch);
  free(log_buffer);
  if (log_fd >= 0) close(log_fd);
  if (logmsg_fp != NULL) fclose(logmsg_fp);
  pthread_mutex_unlock(&log_latch);

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

log_record_t *create_log_update(trx_t *trx, int64_t table_id, pagenum_t page_id,
                                uint16_t offset, uint16_t len, byte *old_img,
                                byte *new_img) {
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

log_record_t *create_log_compensate(trx_t *trx, int64_t table_id,
                                    pagenum_t page_id, uint16_t offset,
                                    uint16_t len, byte *old_img, byte *new_img,
                                    uint64_t next_undo_seq) {
  if (trx == NULL || old_img == NULL || new_img == NULL) {
    LOG_ERR("invalid parameters");
    return NULL;
  }
  auto lsn = LSN++;

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

  pthread_mutex_lock(&log_latch);
  if (log_buffer_size + rec->log_size > log_buffer_max_size) {
    while (log_buffer_size + rec->log_size > log_buffer_max_size)
      log_buffer_max_size *= 2;
    log_buffer = (byte *)realloc(log_buffer, log_buffer_max_size);
  }
  memcpy(log_buffer + log_buffer_size, rec, rec->log_size);
  log_buffer_size += rec->log_size;
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

  if (write(log_fd, log_buffer, log_buffer_size) != log_buffer_size) {
    LOG_ERR("cannot flush log, errno: %s", strerror(errno));
    return 1;
  }
  if (fsync(log_fd) < 0) {
    LOG_ERR("cannot sync log file, errno: %s", strerror(errno));
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
  log_buffer_size = 0;
  pthread_mutex_unlock(&log_latch);
  return 0;
}

void descript_log_file(int n) {
  uint32_t log_size;
  log_record_t *rec = NULL;

  if (lseek(log_fd, 0, SEEK_SET) < 0) {
    LOG_ERR("failed to seek, %s", strerror(errno));
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