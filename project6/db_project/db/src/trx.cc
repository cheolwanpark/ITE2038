#include "trx.h"

#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <map>
#include <stack>
#include <unordered_map>
#include <vector>

#include "buffer_manager.h"
#include "index_manager/bpt.h"
#include "log.h"

// for debugging
#define NTIME_CHECKING
#ifdef TIME_CHECKING
pthread_mutex_t runtime_map_latch = PTHREAD_MUTEX_INITIALIZER;
std::vector<clock_t> lock_acq_runtime;
std::vector<clock_t> lock_acq_wait_time;
std::vector<clock_t> lock_rel_runtime;
std::vector<clock_t> convert_runtime;
std::vector<clock_t> trx_beg_runtime;
std::vector<clock_t> trx_commit_runtime;
std::vector<clock_t> trx_abort_runtime;
std::vector<clock_t> trx_log_runtime;
#endif

// trx, lock relevent datatypes
struct update_log_t {
  int64_t table_id;
  pagenum_t page_id;
  uint16_t offset;
  uint16_t len;
  byte *bef;
  update_log_t *next;
};

struct lock_list_t {
  int64_t table_id;
  pagenum_t page_id;
  lock_t *tail;
  lock_t *head;
};

struct lock_t {
  lock_t *prev;
  lock_t *next;
  lock_list_t *sentinel;
  pthread_cond_t cond;
  int64_t table_id;
  int64_t record_id;
  int lock_mode;
  lock_t *trx_next_lock;
  trx_t *owner_trx;
  uint64_t bitmap;
};

// function definitions
bool is_locking(lock_t *lock, int64_t key, int slotnum);
bool is_trx_assigned(trx_id_t id);
int push_into_trx(trx_t *trx, lock_t *lock);
lock_list_t &get_lock_list(int64_t table_id, pagenum_t page_id);
lock_t *create_lock(int64_t table_id, int64_t record_id, int slotnum, int mode);
void destroy_lock(lock_t *lock);
int __lock_release(lock_t *lock_obj);
int push_into_lock_list(lock_list_t &lock_list, lock_t *lock);
int remove_from_lock_list(lock_t *lock);
int is_conflicting(lock_t *a, lock_t *b);
lock_t *find_conflicting_lock(lock_t *lock);
int is_running(trx_t *trx);
int is_deadlock(trx_t *checking_trx, trx_t *target_trx);
int is_deadlock(lock_t *lock);

// mutexes
#ifdef __unix__
#define PTHREAD_RECURSIVE_MUTEX_INITIALIZER \
  PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP
#endif
pthread_mutex_t trx_table_latch = PTHREAD_RECURSIVE_MUTEX_INITIALIZER;
pthread_mutex_t lock_table_latch = PTHREAD_RECURSIVE_MUTEX_INITIALIZER;

// Transaction API relevent code
int trx_counter = 1;

std::unordered_map<trx_id_t, trx_t *> trx_table;

bool is_locking(lock_t *lock, int64_t key, int slotnum) {
  if (lock == NULL) return false;
  // if (lock->record_id == key) return true;
  if (lock->bitmap & (1ULL << slotnum)) return true;
  return false;
}

bool is_trx_assigned(trx_id_t id) {
  auto found = trx_table.find(id);
  return found != trx_table.end();
}

int push_into_trx(trx_t *trx, lock_t *lock) {
  // mutexes are already locked in lock_acquire
  lock->owner_trx = trx;
  lock->trx_next_lock = trx->head;
  trx->head = lock;
  return 0;
}

int trx_begin() {
#ifdef TIME_CHECKING
  auto start = clock();
#endif

  pthread_mutex_lock(&trx_table_latch);
  while (is_trx_assigned(trx_counter)) {
    ++trx_counter;
    if (trx_counter == INT_MAX) trx_counter = 1;
  }
  trx_t *new_trx = (trx_t *)malloc(sizeof(trx_t));
  if (new_trx == NULL) {
    pthread_mutex_unlock(&trx_table_latch);
    LOG_ERR("failed to allocate new transaction object");
    return 0;
  }
  new_trx->id = trx_counter;
  new_trx->start_time = clock();
  new_trx->head = NULL;
  new_trx->dummy_head = NULL;
  new_trx->log_head = NULL;
  new_trx->releasing = false;
  new_trx->last_lsn = 0;
  auto res = trx_table.emplace(new_trx->id, new_trx);
  if (!res.second) {
    free(new_trx);
    pthread_mutex_unlock(&trx_table_latch);
    LOG_ERR("insertion failed");
    return 0;
  }
  ++trx_counter;
  if (trx_counter == INT_MAX) trx_counter = 1;
  pthread_mutex_unlock(&trx_table_latch);

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  trx_beg_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return new_trx->id;
}

int trx_commit(trx_id_t trx_id) {
#ifdef TIME_CHECKING
  auto start = clock();
#endif

  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&trx_table_latch);
  if (!is_trx_assigned(trx_id)) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("there is no trx with id = %d", trx_id);
    return 0;
  }
  auto *trx = trx_table[trx_id];
  trx->releasing = true;

  // erase from trx table
  trx_table.erase(trx_id);
  pthread_mutex_unlock(&trx_table_latch);

  // release all update logs
  auto *log_iter = trx->log_head;
  while (log_iter != NULL) {
    auto *current = log_iter;
    log_iter = log_iter->next;
    free(current->bef);
    free(current);
  }

  // destroy all dummy locks
  auto *iter = trx->dummy_head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->trx_next_lock;
    destroy_lock(current);
  }

  // release all locks
  iter = trx->head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->trx_next_lock;
    if (__lock_release(current)) {
      pthread_mutex_unlock(&trx_table_latch);
      pthread_mutex_unlock(&lock_table_latch);
      LOG_ERR("failed to release lock");
      return 0;
    }
  }
  pthread_mutex_unlock(&lock_table_latch);

  // free trx
  free(trx);

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  trx_commit_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return trx_id;
}

int trx_abort(trx_id_t trx_id) {
#ifdef TIME_CHECKING
  auto start = clock();
#endif

  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&trx_table_latch);
  if (!is_trx_assigned(trx_id)) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("there is no trx with id = %d", trx_id);
    return 0;
  }
  auto *trx = trx_table[trx_id];
  trx->releasing = true;

  // erase from trx table
  trx_table.erase(trx_id);
  pthread_mutex_unlock(&trx_table_latch);

  // revert all updates
  auto *log_iter = trx->log_head;
  page_t *page;
  while (log_iter != NULL) {
    // overwrite records with log
    page = buffer_get_page_ptr<page_t>(log_iter->table_id, log_iter->page_id);
    memcpy(page->data + log_iter->offset, log_iter->bef, log_iter->len);
    set_dirty((page_t *)page);
    unpin((page_t *)page);

    // update iterator
    auto *current = log_iter;
    log_iter = log_iter->next;

    // free update log
    free(current->bef);
    free(current);
  }

  // destroy all dummy locks
  auto *iter = trx->dummy_head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->trx_next_lock;
    destroy_lock(current);
  }

  // release all locks
  iter = trx->head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->trx_next_lock;
    if (__lock_release(current)) {
      pthread_mutex_unlock(&lock_table_latch);
      LOG_ERR("failed to release lock");
      return 0;
    }
  }
  pthread_mutex_unlock(&lock_table_latch);

  // free trx
  free(trx);

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  trx_abort_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return trx_id;
}

int trx_log_update(trx_t *trx, int64_t table_id, pagenum_t page_id,
                   uint16_t offset, uint16_t len, const byte *bef) {
  if (trx == NULL || bef == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }

#ifdef TIME_CHECKING
  auto start = clock();
#endif

  // create update log object
  update_log_t *result = (update_log_t *)malloc(sizeof(update_log_t));
  if (result == NULL) {
    LOG_ERR("failed to allocate new update log object");
    return 1;
  }
  result->table_id = table_id;
  result->page_id = page_id;
  result->offset = offset;
  result->len = len;
  result->bef = (byte *)malloc(len);
  if (result->bef == NULL) {
    free(result);
    LOG_ERR("failed to allocate bef buffer");
    return 1;
  }
  memcpy(result->bef, bef, len);

  // push it into transaction
  result->next = trx->log_head;
  trx->log_head = result;

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  trx_log_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return 0;
}

// Lock API relevent code
using lock_table_key = std::pair<int64_t, pagenum_t>;
struct lock_table_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &p) const {
    return (p.first << 16) ^ (p.second);
  }
};
std::unordered_map<lock_table_key, lock_list_t, lock_table_hash> lock_table;

lock_list_t &get_lock_list(int64_t table_id, pagenum_t page_id) {
  auto pair_key = std::make_pair(table_id, page_id);
  auto found = lock_table.find(pair_key);
  if (found == lock_table.end()) {
    lock_list_t new_list = {table_id, page_id, NULL, NULL};
    auto res = lock_table.emplace(pair_key, new_list);
    return res.first->second;
  } else
    return found->second;
}

lock_t *create_lock(int64_t table_id, int64_t record_id, int slotnum,
                    int mode) {
  if (mode != S_LOCK && mode != X_LOCK) {
    LOG_ERR("invalid lock mode");
    return NULL;
  }
  lock_t *new_lock = (lock_t *)malloc(sizeof(lock_t));
  if (new_lock == NULL) {
    LOG_ERR("failed to allocate new lock object");
    return NULL;
  }
  new_lock->prev = new_lock->next = NULL;
  new_lock->sentinel = NULL;
  new_lock->cond = PTHREAD_COND_INITIALIZER;
  new_lock->table_id = table_id;
  new_lock->record_id = record_id;
  new_lock->lock_mode = mode;
  new_lock->trx_next_lock = NULL;
  new_lock->owner_trx = NULL;
  new_lock->bitmap = 1ULL << slotnum;
  return new_lock;
}

void destroy_lock(lock_t *lock) {
  if (lock == NULL) return;
  if (pthread_cond_destroy(&lock->cond)) {
    LOG_ERR("failed to destroy condition variable");
    return;
  }
  free(lock);
}

int push_into_lock_list(lock_list_t &lock_list, lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  lock->sentinel = &lock_list;
  lock->next = NULL;
  if (lock_list.tail == NULL && lock_list.head == NULL) {
    lock_list.head = lock_list.tail = lock;
    lock->prev = NULL;
  } else {
    if (lock_list.head == NULL || lock_list.tail == NULL) {
      LOG_ERR("invalid lock list: just one of the head and tail is NULL");
      return 1;
    }
    lock_list.tail->next = lock;
    lock->prev = lock_list.tail;
    lock_list.tail = lock;
  }
  return 0;
}

int remove_from_lock_list(lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  auto *sentinel = lock->sentinel;
  if (lock->prev != NULL) lock->prev->next = lock->next;
  if (lock->next != NULL) lock->next->prev = lock->prev;
  if (sentinel) {
    if (sentinel->head == lock) sentinel->head = lock->next;
    if (sentinel->tail == lock) sentinel->tail = lock->prev;
  }
  return 0;
}

int init_lock_table() {
  pthread_mutex_lock(&trx_table_latch);
  trx_table.clear();
  pthread_mutex_unlock(&trx_table_latch);

  pthread_mutex_lock(&lock_table_latch);
  lock_table.clear();
  pthread_mutex_unlock(&lock_table_latch);

  return 0;
}

int free_lock_table() {
  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&trx_table_latch);
  std::vector<trx_id_t> running_trx_ids;
  for (auto &iter : trx_table) {
    running_trx_ids.push_back(iter.first);
  }
  for (auto trx_id : running_trx_ids) {
    if (trx_abort(trx_id) != trx_id) {
      pthread_mutex_unlock(&trx_table_latch);
      pthread_mutex_unlock(&lock_table_latch);
      LOG_ERR("failed to abort the trx %d", trx_id);
      return 1;
    }
  }
  trx_table.clear();
  pthread_mutex_unlock(&trx_table_latch);
  pthread_mutex_destroy(&trx_table_latch);

  for (auto &iter : lock_table) {
    auto *lock = iter.second.head;
    while (lock != NULL) {
      auto *tmp = lock;
      lock = lock->next;
      if (pthread_cond_destroy(&tmp->cond)) {
        LOG_ERR("failed to destroy condition variable!");
        return 1;
      }
      free(tmp);
    }
  }
  lock_table.clear();
  pthread_mutex_unlock(&lock_table_latch);
  pthread_mutex_destroy(&lock_table_latch);
  return 0;
}

struct leaf_slot_t {
  bpt_key_t key;
  uint16_t size;
  uint16_t offset;
  int32_t trx_id;
} __attribute__((packed));

int convert_implicit_lock(int table_id, pagenum_t page_id, int64_t key,
                          trx_id_t trx_id, int *slotnum) {
#ifdef TIME_CHECKING
  auto start = clock();
#endif

  pthread_mutex_lock(&lock_table_latch);
  pthread_mutex_lock(&trx_table_latch);

  auto *page = buffer_get_page_ptr<bpt_page_t>(table_id, page_id);
  leaf_slot_t *slots = (leaf_slot_t *)(page->page.data + kBptPageHeaderSize);
  auto num_of_keys = page->header.num_of_keys;

  for (*slotnum = 0; *slotnum < num_of_keys; ++(*slotnum)) {
    if (slots[*slotnum].key == key) {
      auto locking_trx_id = slots[*slotnum].trx_id;
      // there is implicit lock, convert it into implicit lock
      // if implicit lock is held by given trx, then do not convert
      if (locking_trx_id != 0 && locking_trx_id != trx_id &&
          is_trx_assigned(locking_trx_id)) {
        // remove implicit lock
        slots[*slotnum].trx_id = 0;
        set_dirty((page_t *)page);
        unpin((page_t *)page);

        // create explicit lock
        // find dummy lock and remove it from dummy lock list
        auto *trx = trx_table[locking_trx_id];
        auto *iter = trx->dummy_head;
        lock_t *prev = NULL;
        while (iter != NULL) {
          if (iter->table_id == table_id && is_locking(iter, key, *slotnum)) {
            // remove it from dummy lock list
            if (prev == NULL)
              trx->dummy_head = iter->trx_next_lock;
            else
              prev->trx_next_lock = iter->trx_next_lock;
            break;
          }
          prev = iter;
          iter = iter->trx_next_lock;
        }
        if (iter == NULL) {
          LOG_ERR("failed to find dummy lock");
          pthread_mutex_unlock(&trx_table_latch);
          pthread_mutex_unlock(&lock_table_latch);
          return 1;
        }

        // push it into trx lock list
        iter->trx_next_lock = trx->head;
        trx->head = iter;

        // push it into lock list
        auto &lock_list = get_lock_list(table_id, page_id);
        if (push_into_lock_list(lock_list, iter)) {
          pthread_mutex_unlock(&trx_table_latch);
          pthread_mutex_unlock(&lock_table_latch);
          LOG_ERR("failed to push into the lock list");
          return 1;
        }

        // after converting, do nothing. (lock will be acquired outside)
        pthread_mutex_unlock(&trx_table_latch);
        pthread_mutex_unlock(&lock_table_latch);
        return 0;
      } else {
        unpin((page_t *)page);
        pthread_mutex_unlock(&trx_table_latch);
        pthread_mutex_unlock(&lock_table_latch);
        return 0;
      }
    }
  }
  *slotnum = -1;
  unpin((page_t *)page);
  pthread_mutex_unlock(&trx_table_latch);
  pthread_mutex_unlock(&lock_table_latch);

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  convert_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return 0;
}

lock_t *try_implicit_lock(int64_t table_id, pagenum_t page_id, int64_t key,
                          int trx_id, int slotnum) {
  pthread_mutex_lock(&lock_table_latch);

  auto &lock_list = get_lock_list(table_id, page_id);

  // check if there is conflicting lock
  auto *iter = lock_list.head;
  while (iter != NULL) {
    // there is lock, so cannot hold implicit lock
    if (is_locking(iter, key, slotnum)) {
      // only same transaction's S lock is allowed
      if (iter->owner_trx->id == trx_id && iter->lock_mode == S_LOCK) {
        iter = iter->next;
        continue;
      } else {
        pthread_mutex_unlock(&lock_table_latch);
        return NULL;
      }
    }
    iter = iter->next;
  }

  pthread_mutex_lock(&trx_table_latch);
  // check if requesting trx is alive
  if (!is_trx_assigned(trx_id)) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("there is no transaction with id = %d", trx_id);
    return NULL;
  }
  auto *trx = trx_table[trx_id];

  // add implicit lock
  // if there was an implicit lock it has already been converted into X lock by
  // convert_implicit_lock
  auto *page = buffer_get_page_ptr<bpt_page_t>(table_id, page_id);
  leaf_slot_t *slots = (leaf_slot_t *)(page->page.data + kBptPageHeaderSize);
  auto num_of_keys = page->header.num_of_keys;

  if (slotnum >= num_of_keys || slots[slotnum].key != key) {
    unpin((page_t *)page);
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("invalid slotnum");
    return NULL;
  }

  slots[slotnum].trx_id = trx_id;
  set_dirty((page_t *)page);
  unpin((page_t *)page);

  // create dummy lock (will not be inserted into lock list)
  auto *new_lock = create_lock(table_id, key, slotnum, X_LOCK);
  if (new_lock == NULL) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to create a new lock");
    return NULL;
  }
  // insert dummy lock into dummy lock list
  new_lock->owner_trx = trx;
  new_lock->trx_next_lock = trx->dummy_head;
  trx->dummy_head = new_lock;
  pthread_mutex_unlock(&trx_table_latch);
  pthread_mutex_unlock(&lock_table_latch);
  return new_lock;
}

lock_t *lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
                     int trx_id, int lock_mode) {
#ifdef TIME_CHECKING
  auto start = clock();
#endif

  pthread_mutex_lock(&lock_table_latch);

  int slotnum = -1;
  if (convert_implicit_lock(table_id, page_id, key, trx_id, &slotnum)) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to convert implicit lock into explicit lock");
    return NULL;
  }

  // there is no record with given key
  if (slotnum < 0) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_WARN("there is no record with given key");
    return NULL;
  }

  if (lock_mode == X_LOCK) {
    pthread_mutex_lock(&trx_table_latch);
    auto *lock = try_implicit_lock(table_id, page_id, key, trx_id, slotnum);
    if (lock != NULL) {
      pthread_mutex_unlock(&trx_table_latch);
      pthread_mutex_unlock(&lock_table_latch);
      return lock;
    }
    pthread_mutex_unlock(&trx_table_latch);
  }

  // check if trx already has a lock
  auto &lock_list = get_lock_list(table_id, page_id);
  auto *iter = lock_list.head;
  while (iter != NULL) {
    if (is_locking(iter, key, slotnum) && iter->lock_mode == lock_mode &&
        iter->owner_trx && iter->owner_trx->id == trx_id) {
      pthread_mutex_unlock(&lock_table_latch);
      return iter;
    }
    iter = iter->next;
  }

  // create and push new lock
  auto *new_lock = create_lock(table_id, key, slotnum, lock_mode);
  new_lock->sentinel = &lock_list;
  if (new_lock == NULL) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to create a new lock");
    return NULL;
  }
  pthread_mutex_lock(&trx_table_latch);
  if (!is_trx_assigned(trx_id)) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    destroy_lock(new_lock);
    return NULL;
  }
  auto *trx = trx_table[trx_id];

  // lock compression
  if (lock_mode == S_LOCK) {
    new_lock->owner_trx = trx;
    // there is no conflicting, can do lock compression
    if (find_conflicting_lock(new_lock) == NULL) {
      auto *iter = lock_list.head;
      while (iter != NULL) {
        if (iter->lock_mode == S_LOCK && iter->owner_trx &&
            iter->owner_trx->id == trx_id)
          break;
        iter = iter->next;
      }
      if (iter != NULL) {
        destroy_lock(new_lock);
        iter->bitmap |= 1ULL << slotnum;
        pthread_mutex_unlock(&trx_table_latch);
        pthread_mutex_unlock(&lock_table_latch);
        return iter;
      }
      // if there is no S lock on trx, then create new one
    }
  }

  if (push_into_trx(trx, new_lock)) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    destroy_lock(new_lock);
    return NULL;
  }
  if (is_deadlock(new_lock)) {
    if (trx_abort(trx_id) != trx_id) LOG_WARN("failed to abort trx");
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    return NULL;
  }
  pthread_mutex_unlock(&trx_table_latch);
  if (push_into_lock_list(lock_list, new_lock)) {
    pthread_mutex_unlock(&lock_table_latch);
    destroy_lock(new_lock);
    LOG_ERR("failed to push into the lock list");
    return NULL;
  }

#ifdef TIME_CHECKING
  auto start2 = clock();
#endif

  if (find_conflicting_lock(new_lock) != NULL) {
    pthread_cond_wait(&new_lock->cond, &lock_table_latch);
  }

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  lock_acq_wait_time.push_back(clock() - start2);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  pthread_mutex_unlock(&lock_table_latch);

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  lock_acq_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return new_lock;
};

int __lock_release(lock_t *lock_obj) {
  if (lock_obj == NULL) {
    LOG_WARN("invalid parameters");
    return 1;
  }

  auto *iter = lock_obj->next;
  auto rid = lock_obj->record_id;
  auto bitmap = lock_obj->bitmap;
  if (remove_from_lock_list(lock_obj)) {
    LOG_WARN("failed to remove from the lock list");
    return 1;
  }
  destroy_lock(lock_obj);
  while (iter != NULL) {
    if (iter->bitmap & bitmap && find_conflicting_lock(iter) == NULL)
      pthread_cond_signal(&iter->cond);
    iter = iter->next;
  }
  return 0;
}

int lock_release(lock_t *lock_obj) {
  if (lock_obj == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }

#ifdef TIME_CHECKING
  auto start = clock();
#endif

  pthread_mutex_lock(&lock_table_latch);
  if (__lock_release(lock_obj)) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to release lock");
    return 1;
  }
  pthread_mutex_unlock(&lock_table_latch);

#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  lock_rel_runtime.push_back(clock() - start);
  pthread_mutex_unlock(&runtime_map_latch);
#endif

  return 0;
}

trx_t *get_trx(lock_t *lock) {
  if (lock == NULL) return NULL;
  return lock->owner_trx;
}

int is_conflicting(lock_t *a, lock_t *b) {
  if (a == NULL || b == NULL) {
    LOG_ERR("invalid parameters");
    return false;
  }

  if (a->sentinel == NULL || a->sentinel != b->sentinel) {
    return false;
  }

  if ((a->bitmap & b->bitmap) == 0) return false;
  if (a->owner_trx->id == b->owner_trx->id) return false;

  if (a->lock_mode == X_LOCK || b->lock_mode == X_LOCK)
    return true;
  else
    return false;
}

lock_t *find_conflicting_lock(lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return NULL;
  }
  if (lock->sentinel == NULL) {
    return NULL;
  }

  auto *iter = lock->sentinel->head;
  while (iter != NULL && iter != lock) {
    if (is_conflicting(iter, lock)) return iter;
    iter = iter->next;
  }
  return NULL;
}

int is_running(trx_t *trx) {
  // latches are already locked in is_deadlock
  auto *iter = trx->head;
  while (iter != NULL) {
    if (find_conflicting_lock(iter) != NULL) return false;
    iter = iter->trx_next_lock;
  }
  return true;
}

int is_deadlock(trx_t *checking_trx, trx_t *target_trx) {
  // latches are already locked in is_deadlock
  // if target trx is committed/aborted or not waiting, then not a deadlock
  if (!is_trx_assigned(target_trx->id) || is_running(target_trx)) return false;
  auto *trx_lock_iter = target_trx->head;
  while (trx_lock_iter != NULL) {
    auto *sentinel = trx_lock_iter->sentinel;
    if (sentinel) {
      auto *iter = sentinel->head;
      while (iter != NULL && iter != trx_lock_iter) {
        if (is_conflicting(iter, trx_lock_iter)) {
          if (iter->owner_trx->id == checking_trx->id) {
            return true;
          } else if (is_deadlock(checking_trx, iter->owner_trx))
            return true;
          // we don't have to check locks after first x lock
          if (iter->lock_mode == X_LOCK) break;
        }
        iter = iter->next;
      }
    }
    trx_lock_iter = trx_lock_iter->trx_next_lock;
  }
  return false;
}

int is_deadlock(lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return false;
  }
  if (lock->sentinel == NULL || lock->owner_trx == NULL) {
    return false;
  }
  // mutexes are already locked in lock_acquire

  auto *sentinel = lock->sentinel;
  auto *checking_trx = lock->owner_trx;
  if (sentinel) {
    auto *iter = sentinel->head;
    while (iter != NULL && iter != lock) {
      if (is_conflicting(iter, lock) && !iter->owner_trx->releasing) {
        if (is_deadlock(checking_trx, iter->owner_trx)) return true;
        // we don't have to check locks after first x lock
        if (iter->lock_mode == X_LOCK) break;
      }
      iter = iter->next;
    }
  }
  return false;
}

double calc_avg(const std::vector<clock_t> &v) {
  double result = 0.0;
  for (auto &val : v) {
    result += (double)val / (double)v.size();
  }
  return (double)result / (double)CLOCKS_PER_SEC;
}

void print_debugging_infos() {
#ifdef TIME_CHECKING
  pthread_mutex_lock(&runtime_map_latch);
  LOG_INFO("lock acq runtime : %.10llf", calc_avg(lock_acq_runtime));
  LOG_INFO("lock acq wait time : %.10llf", calc_avg(lock_acq_wait_time));
  LOG_INFO("lock rel runtime : %.10llf", calc_avg(lock_rel_runtime));
  LOG_INFO("convert runtime : %.10llf", calc_avg(convert_runtime));
  LOG_INFO("trx beg runtime : %.10llf", calc_avg(trx_beg_runtime));
  LOG_INFO("trx com runtime : %.10llf", calc_avg(trx_commit_runtime));
  LOG_INFO("trx abo runtime : %.10llf", calc_avg(trx_abort_runtime));
  LOG_INFO("trx log runtime : %.10llf", calc_avg(trx_log_runtime));
  pthread_mutex_unlock(&runtime_map_latch);
#endif
}