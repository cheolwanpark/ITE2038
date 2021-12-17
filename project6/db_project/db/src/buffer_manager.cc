#include "buffer_manager.h"

#include <pthread.h>
#include <string.h>

#include <algorithm>
#include <unordered_map>

#include "log.h"
#include "recovery.h"

struct frame_t {
  page_t frame;
  int64_t table_id;
  pagenum_t page_num;
  int8_t is_dirty;
  pthread_mutex_t page_latch;
  frame_t *next;
  frame_t *prev;
};

using frame_id_t = std::pair<int64_t, pagenum_t>;
struct frame_map_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &p) const {
    return (p.first << 16) ^ (p.second);
  }
};
using frame_map_t = std::unordered_map<frame_id_t, frame_t *, frame_map_hash>;
frame_map_t frame_map;
frame_t **frame_cache = NULL;
uint32_t cache_size = 0;
frame_t *frames = NULL, *head = NULL, *tail = NULL;

pthread_mutex_t buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t frame_map_latch = PTHREAD_MUTEX_INITIALIZER;

// load page into buffer
// return loaded frame ptr (NULL on failed)
frame_t *buffer_load_page(int64_t table_id, pagenum_t pagenum);

// evict page (clock policy)
// return evicted page's frame ptr (to load page into that position)
// return NULL on failed
frame_t *buffer_evict_frame();

// find specific frame in buffer
// return NULL on failed
frame_t *find_frame(int64_t table_id, pagenum_t pagenum);

// make given node a head of the LRU list
void set_LRU_head(frame_t *node);

// make given node a tail of the LRU list
void set_LRU_tail(frame_t *node);

// get frame ptr and update LRU list
frame_t *get_frame(int64_t table_id, pagenum_t pagenum);

// internal api functions
// to preserve interface , Disk Space Manager uses this functions internally
void __buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t *dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  const page_t *page_ptr = buffer_get_page_ptr(table_id, pagenum);
  if (page_ptr == NULL) {
    LOG_ERR(3, "failed to get page pointer");
    return;
  }

  memcpy(dest, page_ptr, sizeof(page_t));
}

void __buffer_write_page(int64_t table_id, pagenum_t pagenum,
                         const page_t *src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  auto frame = find_frame(table_id, pagenum);
  if (frame == NULL) {
    LOG_WARN("there is no frame with key=(%lld %llu) in the buffer", table_id,
             pagenum);
    return;
  }
  memcpy(&frame->frame, src, sizeof(page_t));
  frame->is_dirty = true;
}

void __unpin(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  auto *frame = find_frame(table_id, pagenum);
  if (frame == NULL) {
    LOG_WARN("there is no frame with key=(%lld %llu) in the buffer", table_id,
             pagenum);
    return;
  }
  if (pthread_mutex_unlock(&frame->page_latch)) {
    LOG_ERR(3, "failed to unlock page latch");
    return;
  }
}

frame_t *buffer_load_page(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return NULL;
  }

  // buffer_manager_latch is already locked in buffer_get_page_ptr
  frame_t *frame = buffer_evict_frame();
  if (frame == NULL) {
    LOG_ERR(3, "failed to evict frame");
    return NULL;
  }
  frame->table_id = table_id;
  frame->page_num = pagenum;
  file_read_page(table_id, pagenum, &frame->frame);

  // push to frame_map
  auto frame_id = std::make_pair(table_id, pagenum);
  pthread_mutex_lock(&frame_map_latch);
  auto res = frame_map.emplace(frame_id, frame);
  if (!res.second) {
    pthread_mutex_unlock(&frame_map_latch);
    LOG_ERR(3, "failed to emplace into the frame map");
    return NULL;
  }
  auto hash_key = frame_map.hash_function()(frame_id);
  frame_cache[hash_key % cache_size] = frame;
  pthread_mutex_unlock(&frame_map_latch);
  return frame;
}

frame_t *buffer_evict_frame() {
  // find evict page
  // buffer_manager_latch is already locked in buffer_get_page_ptr
  auto iter = tail;
  while (iter != NULL) {
    if (pthread_mutex_trylock(&iter->page_latch) == 0)
      break;
    else
      iter = iter->prev;
  }
  if (iter == NULL) {
    LOG_ERR(3, "all buffer frame is pinned, cannot evict frame");
    return NULL;
  }

  // flush if dirty flag set
  if (iter->is_dirty) {
    // flush all logs
    if (flush_log()) {
      LOG_ERR(3, "failed to flush logs");
      return NULL;
    }

    file_write_page(iter->table_id, iter->page_num, &iter->frame);
  }

  // remove from frame_map
  auto frame_id = std::make_pair(iter->table_id, iter->page_num);
  pthread_mutex_lock(&frame_map_latch);
  frame_map.erase(frame_id);
  auto hash_key = frame_map.hash_function()(frame_id);
  auto *cached_frame = frame_cache[hash_key % cache_size];
  if (cached_frame != NULL && cached_frame->table_id == frame_id.first &&
      cached_frame->page_num == frame_id.second)
    frame_cache[hash_key % cache_size] = NULL;
  pthread_mutex_unlock(&frame_map_latch);

  // reset frame data
  iter->table_id = -1;
  iter->page_num = 0;
  iter->is_dirty = false;
  pthread_mutex_unlock(&iter->page_latch);
  return iter;
}

page_t *buffer_get_page_ptr(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return NULL;
  }

  pthread_mutex_lock(&buffer_manager_latch);
  auto result = find_frame(table_id, pagenum);
  if (result == NULL) {
    result = buffer_load_page(table_id, pagenum);
    if (result == NULL) {
      pthread_mutex_unlock(&buffer_manager_latch);
      LOG_ERR(3, "failed to load page");
      return NULL;
    }
  }

  set_LRU_head(result);
  if (pthread_mutex_lock(&result->page_latch)) {
    LOG_ERR(3, "failed to lock page latch");
    return NULL;
  }
  pthread_mutex_unlock(&buffer_manager_latch);

  return &result->frame;
}

void set_dirty(page_t *page) {
  if (page == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  frame_t *frame = (frame_t *)page;
  frame->is_dirty = true;
}

frame_t *find_frame(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return NULL;
  }

  auto frame_id = std::make_pair(table_id, pagenum);
  pthread_mutex_lock(&frame_map_latch);
  // check cache
  auto hash_key = frame_map.hash_function()(frame_id);
  auto *cached_frame = frame_cache[hash_key % cache_size];
  if (cached_frame != NULL && cached_frame->table_id == frame_id.first &&
      cached_frame->page_num == frame_id.second) {
    pthread_mutex_unlock(&frame_map_latch);
    return cached_frame;
  }

  auto search = frame_map.find(frame_id);
  if (search != frame_map.end()) {
    frame_cache[hash_key % cache_size] = search->second;
    pthread_mutex_unlock(&frame_map_latch);
    return search->second;
  } else {
    pthread_mutex_unlock(&frame_map_latch);
    return NULL;
  }
}

void set_LRU_head(frame_t *node) {
  if (node == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  // buffer_manager_latch is already locked in buffer_get_page_ptr
  if (node->prev != NULL) {    // node is not a head
    if (node->next == NULL) {  // node is a tail
      tail = node->prev;
      tail->next = NULL;

      head->prev = node;
      node->next = head;
      node->prev = NULL;
      head = node;
    } else {  // result is not a head and result is not a tail
      node->prev->next = node->next;
      node->next->prev = node->prev;

      head->prev = node;
      node->next = head;
      node->prev = NULL;
      head = node;
    }
  }
  // if node is head, then do nothing
}

void set_LRU_tail(frame_t *node) {
  if (node == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  // buffer_manager_latch is already locked in buffer_free_page
  if (node->next != NULL) {    // node is not a tail
    if (node->prev == NULL) {  // node is a head
      head = node->next;
      head->prev = NULL;

      tail->next = node;
      node->prev = tail;
      node->next = NULL;
      tail = node;
    } else {  // node is not a tail and node is not a head
      node->prev->next = node->next;
      node->next->prev = node->prev;

      tail->next = node;
      node->prev = tail;
      node->next = NULL;
      tail = node;
    }
  }
  // if node is tail, then do nothing
}

int init_buffer_manager(int num_buf) {
  if (num_buf < 1) {
    LOG_ERR(3, "invalid parameters");
    return 1;
  }

  pthread_mutex_lock(&buffer_manager_latch);
  frames = (frame_t *)malloc(num_buf * sizeof(frame_t));
  if (frames == NULL) {
    LOG_ERR(3, "failed to allocate buffer frames");
    return 1;
  }

  head = frames;
  tail = &frames[num_buf - 1];

  // initialize as empty frame and create list
  for (int i = 0; i < num_buf; ++i) {
    frames[i].table_id = -1;
    frames[i].page_num = 0;
    frames[i].is_dirty = false;
    frames[i].page_latch = PTHREAD_MUTEX_INITIALIZER;
    frames[i].next = i + 1 < num_buf ? &frames[i + 1] : NULL;
    frames[i].prev = i - 1 < 0 ? NULL : &frames[i - 1];
  }
  pthread_mutex_lock(&frame_map_latch);
  frame_map.clear();
  cache_size = num_buf;
  frame_cache = (frame_t **)malloc(sizeof(frame_t *) * cache_size);
  memset(frame_cache, 0, sizeof(frame_t *) * cache_size);
  pthread_mutex_unlock(&frame_map_latch);
  pthread_mutex_unlock(&buffer_manager_latch);
  return 0;
}

int free_buffer_manager() {
  pthread_mutex_lock(&buffer_manager_latch);
  // write all dirty frames
  auto iter = head;
  for (auto iter = head; iter != NULL; iter = iter->next) {
    if (iter->is_dirty)
      file_write_page(iter->table_id, iter->page_num, &iter->frame);
    if (pthread_mutex_destroy(&iter->page_latch)) {
      LOG_WARN("failed to destroy page latch, %s", strerror(errno));
    }
  }

  // free resources
  if (frames != NULL) free(frames);
  pthread_mutex_lock(&frame_map_latch);
  frame_map.clear();
  if (frame_cache != NULL) free(frame_cache);
  pthread_mutex_unlock(&frame_map_latch);
  pthread_mutex_unlock(&buffer_manager_latch);
  return 0;
}

pagenum_t buffer_alloc_page(int64_t table_id) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return 0;
  }

  auto *header_page =
      buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  if (header_page->header.first_free_page == 0) {
    pagenum_t start = 0, end = 0;
    uint64_t num_new_pages = 0;
    if (file_expand_twice(table_id, &start, &end, &num_new_pages) ||
        start == 0) {
      unpin(header_page);
      LOG_ERR(3, "failed to expand file");
      return 0;
    }
    // connect expanded list into header page
    header_page->header.first_free_page = start;
    header_page->header.num_of_pages += num_new_pages;
  }

  auto result = header_page->header.first_free_page;
  auto *allocated_page = buffer_get_page_ptr<page_node_t>(table_id, result);
  header_page->header.first_free_page = allocated_page->next_free_page;
  unpin(allocated_page);

  set_dirty(header_page);
  unpin(header_page);
  return result;
}

void buffer_free_page(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0 || pagenum < 1) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  auto *header_page =
      buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
  auto *page_node = buffer_get_page_ptr<page_node_t>(table_id, pagenum);

  page_node->next_free_page = header_page->header.first_free_page;
  header_page->header.first_free_page = pagenum;

  set_dirty(header_page);
  set_dirty(page_node);
  unpin(header_page);
  unpin(page_node);

  pthread_mutex_lock(&buffer_manager_latch);
  auto freed_frame = find_frame(table_id, pagenum);
  if (freed_frame != NULL) set_LRU_tail(freed_frame);
  pthread_mutex_unlock(&buffer_manager_latch);
}

void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t *dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  __buffer_read_page(table_id, pagenum, dest);
}

void buffer_read_header_page(int64_t table_id, header_page_t *dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  __buffer_read_page(table_id, kHeaderPagenum, (page_t *)dest);
}

void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t *src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  return __buffer_write_page(table_id, pagenum, src);
}

void buffer_write_header_page(int64_t table_id, const header_page_t *src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  __buffer_write_page(table_id, kHeaderPagenum, (page_t *)src);
}

void unpin(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  __unpin(table_id, pagenum);
}

void unpin(page_t *page) {
  if (page == NULL) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  frame_t *frame = (frame_t *)page;
  if (pthread_mutex_unlock(&frame->page_latch)) {
    LOG_ERR(3, "failed to unlock page latch");
    return;
  }
}

void unpin_header(int64_t table_id) {
  if (table_id < 0) {
    LOG_ERR(3, "invalid parameters");
    return;
  }

  __unpin(table_id, kHeaderPagenum);
}

int count_free_frames() {
  pthread_mutex_lock(&buffer_manager_latch);
  int result = 0;
  for (auto iter = head; iter != NULL; iter = iter->next) {
    if (pthread_mutex_trylock(&iter->page_latch) == 0) {
      ++result;
      pthread_mutex_unlock(&iter->page_latch);
    }
  }
  pthread_mutex_unlock(&buffer_manager_latch);
  return result;
}

int buffer_flush_all_frames() {
  pthread_mutex_lock(&buffer_manager_latch);
  auto iter = head;
  for (auto iter = head; iter != NULL; iter = iter->next) {
    if (iter->is_dirty)
      file_write_page(iter->table_id, iter->page_num, &iter->frame);
    pthread_mutex_unlock(&iter->page_latch);
  }
  pthread_mutex_unlock(&buffer_manager_latch);
  return 0;
}