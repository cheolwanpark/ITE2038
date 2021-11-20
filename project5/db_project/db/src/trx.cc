#include "trx.h"

#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <unordered_map>

#include "log.h"

// trx, lock relevent datatypes
struct trx_t {
  trx_id_t id;
  lock_t *head;
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
  int64_t record_id;
  int lock_mode;
  lock_t *trx_next_lock;
  lock_t *trx_prev_lock;
  trx_t *owner_trx;
};

// function definitions
bool is_trx_assigned(trx_id_t id);
int push_into_trx(trx_id_t trx_id, lock_t *lock);
lock_list_t &get_lock_list(int64_t table_id, pagenum_t page_id);
lock_t *create_lock(int64_t record_id, int mode);
void destroy_lock(lock_t *lock);
int push_into_lock_list(lock_list_t &lock_list, lock_t *lock);
int remove_from_lock_list(lock_t *lock);
int wait_if_conflict(lock_t *lock);
int awake_if_not_conflict(lock_t *lock);

// mutexes
pthread_mutex_t trx_table_latch = PTHREAD_RECURSIVE_MUTEX_INITIALIZER;
pthread_mutex_t lock_table_latch = PTHREAD_RECURSIVE_MUTEX_INITIALIZER;

// Transaction API relevent code
int trx_counter = 1;

std::unordered_map<trx_id_t, trx_t *> trx_table;

bool is_trx_assigned(trx_id_t id) {
  auto found = trx_table.find(id);
  return found != trx_table.end();
}

int push_into_trx(trx_id_t trx_id, lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  pthread_mutex_lock(&trx_table_latch);
  if (!is_trx_assigned(trx_id)) {
    pthread_mutex_unlock(&trx_table_latch);
    LOG_ERR("there is no trx with id = %d", trx_id);
    return 1;
  }
  auto *trx = trx_table[trx_id];
  lock->owner_trx = trx;
  lock->trx_prev_lock = NULL;
  lock->trx_next_lock = trx->head;
  if (trx->head != NULL) trx->head->trx_prev_lock = lock;
  trx->head = lock;
  pthread_mutex_unlock(&trx_table_latch);
  return 0;
}

int trx_begin() {
  pthread_mutex_lock(&trx_table_latch);
  while (is_trx_assigned(trx_counter)) {
    ++trx_counter;
    if (trx_counter == INT_MAX) trx_counter = 1;
  }
  trx_t *new_trx = (trx_t *)malloc(sizeof(trx_t));
  if (new_trx == NULL) {
    LOG_ERR("failed to allocate new transaction object");
    return 0;
  }
  new_trx->id = trx_counter;
  new_trx->head = NULL;
  auto res = trx_table.emplace(new_trx->id, new_trx);
  if (!res.second) {
    LOG_ERR("insertion failed");
    return 0;
  }
  ++trx_counter;
  if (trx_counter == INT_MAX) trx_counter = 1;
  pthread_mutex_unlock(&trx_table_latch);
  return new_trx->id;
}

int trx_commit(trx_id_t trx_id) {
  pthread_mutex_lock(&lock_table_latch);  // deadlock prevention
  pthread_mutex_lock(&trx_table_latch);
  if (!is_trx_assigned(trx_id)) {
    LOG_ERR("there is no trx with id = %d", trx_id);
    return 0;
  }
  auto *trx = trx_table[trx_id];
  auto *iter = trx->head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->trx_next_lock;
    if (lock_release(current)) {
      pthread_mutex_unlock(&trx_table_latch);
      pthread_mutex_unlock(&lock_table_latch);
      LOG_ERR("failed to release lock");
      return 0;
    }
  }
  trx_table.erase(trx_id);
  pthread_mutex_unlock(&trx_table_latch);
  pthread_mutex_unlock(&lock_table_latch);
  return trx_id;
}

int trx_abort_without_erase(trx_id_t trx_id) {
  // TODO: add data revert code
  if (!is_trx_assigned(trx_id)) {
    LOG_ERR("there is no trx with id = %d", trx_id);
    return 0;
  }
  auto *trx = trx_table[trx_id];
  auto *iter = trx->head;
  while (iter != NULL) {
    auto *current = iter;
    iter = iter->trx_next_lock;
    if (lock_release(current)) {
      LOG_ERR("failed to release lock");
      return 0;
    }
  }
  free(trx);
  return trx_id;
}

int trx_abort(trx_id_t trx_id) {
  pthread_mutex_lock(&lock_table_latch);  // deadlock prevention
  pthread_mutex_lock(&trx_table_latch);
  // TODO: add data revert code
  if (!is_trx_assigned(trx_id)) {
    LOG_ERR("there is no trx with id = %d", trx_id);
    return 0;
  }
  if (trx_abort_without_erase(trx_id) == 0) {
    pthread_mutex_unlock(&trx_table_latch);
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to abort trx");
    return 0;
  }
  trx_table.erase(trx_id);
  pthread_mutex_unlock(&trx_table_latch);
  pthread_mutex_unlock(&lock_table_latch);
  return trx_id;
}

// Lock API relevent code
using lock_table_key = std::pair<int64_t, pagenum_t>;
struct lock_table_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &p) const {
    return (p.first & 0x1f) | (p.second << 5);
  }
};
std::unordered_map<lock_table_key, lock_list_t, lock_table_hash> lock_table;

lock_list_t &get_lock_list(int64_t table_id, pagenum_t page_id) {
  auto pair_key = std::make_pair(table_id, page_id);
  auto found = lock_table.find(pair_key);
  if (found == lock_table.end()) {
    lock_list_t new_list = {table_id, page_id, NULL, NULL};
    lock_table.emplace(pair_key, new_list);
  }
  return lock_table[pair_key];
}

lock_t *create_lock(int64_t record_id, int mode) {
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
  new_lock->record_id = record_id;
  new_lock->lock_mode = mode;
  new_lock->trx_next_lock = new_lock->trx_prev_lock = NULL;
  new_lock->owner_trx = NULL;
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
  if (lock == NULL || lock->sentinel == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  auto *sentinel = lock->sentinel;
  if (lock->prev != NULL) lock->prev->next = lock->next;
  if (lock->next != NULL) lock->next->prev = lock->prev;
  if (sentinel->head == lock) sentinel->head = lock->next;
  if (sentinel->tail == lock) sentinel->tail = lock->prev;
  return 0;
}

int wait_if_conflict(lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  if (lock->sentinel == NULL) {
    LOG_ERR("invalid lock object: sentinel is NULL");
    return 1;
  }

  auto *iter = lock->sentinel->head;
  while (iter != NULL && iter != lock) {
    if (iter->record_id == lock->record_id &&
        iter->owner_trx != lock->owner_trx) {
      if (lock->lock_mode == X_LOCK || iter->lock_mode == X_LOCK) {
        pthread_cond_wait(&lock->cond, &lock_table_latch);
        break;
      }
    }
    iter = iter->next;
  }

  return 0;
}

int awake_if_not_conflict(lock_t *lock) {
  if (lock == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }
  if (lock->sentinel == NULL) {
    LOG_ERR("invalid lock object: sentinel is NULL");
    return 1;
  }

  auto *iter = lock->sentinel->head;
  while (iter != NULL && iter != lock) {
    if (iter->record_id == lock->record_id &&
        iter->owner_trx != lock->owner_trx) {
      if (lock->lock_mode == X_LOCK || iter->lock_mode == X_LOCK) return 0;
    }
    iter = iter->next;
  }
  if (iter == lock) {
    pthread_cond_signal(&lock->cond);
  }
  return 0;
}

int init_lock_table() { return 0; }

int free_lock_table() {
  pthread_mutex_lock(&trx_table_latch);
  for (auto &iter : trx_table) {
    if (trx_abort_without_erase(iter.second->id) == 0) {
      return 1;
    }
  }
  trx_table.clear();
  pthread_mutex_unlock(&trx_table_latch);
  pthread_mutex_destroy(&trx_table_latch);

  pthread_mutex_lock(&lock_table_latch);
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

lock_t *lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
                     int trx_id, int lock_mode) {
  pthread_mutex_lock(&lock_table_latch);
  auto &lock_list = get_lock_list(table_id, page_id);

  // check if trx already has a lock
  auto *iter = lock_list.head;
  while (iter != NULL) {
    if (iter->record_id == key && lock_mode == iter->lock_mode &&
        iter->owner_trx && iter->owner_trx->id == trx_id) {
      pthread_mutex_unlock(&lock_table_latch);
      return iter;
      // if (lock_mode == X_LOCK && iter->lock_mode == S_LOCK) {
      //   auto *lock = iter;
      //   // find last S lock with given key
      //   lock_t *last_s_lock = NULL;
      //   iter = lock_list.head;
      //   while (iter != NULL) {
      //     if (iter != lock && iter->record_id == key) {
      //       if (iter->lock_mode == S_LOCK)
      //         last_s_lock = iter;
      //       else
      //         break;
      //     }
      //     iter = iter->next;
      //   }
      //   lock->lock_mode = X_LOCK;
      //   if (last_s_lock) {  // there is s lock with given key
      //     if (remove_from_lock_list(lock)) {
      //       pthread_mutex_unlock(&lock_table_latch);
      //       LOG_ERR("failed to remove from the lock list");
      //       return NULL;
      //     }

      //     lock->next = last_s_lock->next;
      //     if (last_s_lock->next) last_s_lock->next->prev = lock;
      //     last_s_lock->next = lock;
      //     lock->prev = last_s_lock;
      //     if (last_s_lock == lock_list.tail) lock_list.tail = lock;
      //     if (wait_if_conflict(lock)) {
      //       pthread_mutex_unlock(&lock_table_latch);
      //       LOG_ERR("failed to make wait conflicting lock");
      //       return NULL;
      //     }
      //   }
      //   pthread_mutex_unlock(&lock_table_latch);
      //   return lock;
      // } else {
      //   pthread_mutex_unlock(&lock_table_latch);
      //   return iter;
      // }
    }
    iter = iter->next;
  }

  // create and push new lock
  auto *new_lock = create_lock(key, lock_mode);
  if (new_lock == NULL) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to create a new lock");
    return NULL;
  }
  if (push_into_lock_list(lock_list, new_lock)) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to push into the lock list");
    destroy_lock(new_lock);
    return NULL;
  }
  if (push_into_trx(trx_id, new_lock)) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to push into the trx");
    destroy_lock(new_lock);
    return NULL;
  }
  if (wait_if_conflict(new_lock)) {
    pthread_mutex_unlock(&lock_table_latch);
    LOG_ERR("failed to make wait conflicting lock");
    return NULL;
  }
  pthread_mutex_unlock(&lock_table_latch);
  return new_lock;
};

int lock_release(lock_t *lock_obj) {
  if (lock_obj == NULL) {
    LOG_ERR("invalid parameters");
    return 1;
  }

  pthread_mutex_lock(&lock_table_latch);
  auto *iter = lock_obj->next;
  auto rid = lock_obj->record_id;
  if (remove_from_lock_list(lock_obj)) {
    LOG_WARN("failed to remove from the lock list");
    return 1;
  }
  destroy_lock(lock_obj);
  while (iter != NULL) {
    if (iter->record_id == rid && awake_if_not_conflict(iter)) {
      LOG_WARN("failed to awake non-conflicting locks");
      return 1;
    }
    iter = iter->next;
  }
  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}
