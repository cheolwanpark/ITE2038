#include "lock_table.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include <unordered_map>

typedef struct lock_list_t {
  int64_t table_id;
  int64_t record_id;
  lock_t *tail;
  lock_t *head;
} lock_list_t;

struct lock_t {
  lock_t *prev;
  lock_t *next;
  lock_list_t *sentinel;
  pthread_cond_t cond;
};

typedef struct lock_t lock_t;

pthread_mutex_t lock_table_latch = PTHREAD_MUTEX_INITIALIZER;

using lock_table_key = std::pair<int64_t, int64_t>;
struct lock_table_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &p) const {
    return (p.first & 0x1f) | (p.second << 5);
  }
};
std::unordered_map<lock_table_key, lock_list_t, lock_table_hash> lock_table;

lock_list_t &get_lock_list(int64_t table_id, int64_t key) {
  auto pair_key = std::make_pair(table_id, key);
  auto found = lock_table.find(pair_key);
  if (found == lock_table.end()) {
    lock_list_t new_list = {table_id, key, NULL, NULL};
    lock_table.emplace(pair_key, new_list);
  }
  return lock_table[pair_key];
}

lock_t *add_lock(lock_list_t &lock_list) {
  lock_t *new_lock = (lock_t *)malloc(sizeof(lock_t));
  if (new_lock == NULL) {
    fprintf(stderr, "failed to allocate new lock object");
    return NULL;
  }
  new_lock->cond = PTHREAD_COND_INITIALIZER;
  new_lock->sentinel = &lock_list;
  new_lock->next = NULL;
  if (lock_list.tail == NULL && lock_list.head == NULL) {
    lock_list.head = lock_list.tail = new_lock;
    new_lock->prev = NULL;
  } else {
    lock_list.tail->next = new_lock;
    new_lock->prev = lock_list.tail;
    lock_list.tail = new_lock;
    pthread_cond_wait(&new_lock->cond, &lock_table_latch);
  }
  return new_lock;
}

int init_lock_table() { return 0; }

int free_lock_table() {
  pthread_mutex_lock(&lock_table_latch);
  for (auto &iter : lock_table) {
    auto *lock = iter.second.head;
    while (lock != NULL) {
      auto *tmp = lock;
      lock = lock->next;
      if (pthread_cond_destroy(&tmp->cond)) {
        fprintf(stderr, "failed to destroy condition variable!\n");
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

lock_t *lock_acquire(int64_t table_id, int64_t key) {
  pthread_mutex_lock(&lock_table_latch);
  auto &lock_list = get_lock_list(table_id, key);
  auto *new_lock = add_lock(lock_list);
  pthread_mutex_unlock(&lock_table_latch);
  return new_lock;
};

int lock_release(lock_t *lock_obj) {
  pthread_mutex_lock(&lock_table_latch);
  auto *sentinel = lock_obj->sentinel;
  if (sentinel->head != lock_obj) {
    fprintf(stderr,
            "invalid lock list: lock_release call of non-head lock object\n");
    return 1;
  }
  sentinel->head = lock_obj->next;
  if (sentinel->head) {
    sentinel->head->prev = NULL;
    if (pthread_cond_signal(&sentinel->head->cond)) {
      fprintf(stderr, "failed to signal cond variable");
      return 1;
    }
  } else {
    if (sentinel->tail != lock_obj) {
      fprintf(
          stderr,
          "invalid lock list: lock object whose next is null is not a tail\n");
      return 1;
    }
    sentinel->tail = NULL;
  }
  pthread_cond_destroy(&lock_obj->cond);
  free(lock_obj);
  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}
