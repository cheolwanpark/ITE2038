#include "buffer_manager.h"

#include <algorithm>
#include <map>

#include "log.h"

struct frame_t {
  page_t frame;
  int64_t table_id;
  pagenum_t page_num;
  int8_t is_dirty;
  int16_t pin_count;
  frame_t *next;
  frame_t *prev;
};

using frame_id_t = std::pair<int64_t, pagenum_t>;
using frame_map_t = std::map<frame_id_t, frame_t *>;
frame_map_t frame_map;
frame_t *frames = NULL, *head = NULL, *tail = NULL;

// load page into buffer
// return loaded frame ptr (NULL on failed)
frame_t *buffer_load_page(int64_t table_id, pagenum_t pagenum);

// evict page (clock policy)
// return evicted page's frame ptr (to load page into that position)
// return NULL on failed
frame_t *buffer_evict_frame();

// get frame ptr (if there is no corresponding frame in buffer then load it)
// return NULL on failed
const page_t *buffer_get_page_ptr(int64_t table_id, pagenum_t pagenum);

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
    LOG_ERR("invalid parameters");
    return;
  }

  const page_t *page_ptr = buffer_get_page_ptr(table_id, pagenum);
  if (page_ptr == NULL) {
    LOG_ERR("failed to get page pointer");
    return;
  }

  memcpy(dest, page_ptr, sizeof(page_t));
}

void __buffer_write_page(int64_t table_id, pagenum_t pagenum,
                         const page_t *src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

  auto frame = find_frame(table_id, pagenum);
  if (frame == NULL) {
    LOG_WARN("there is no frame in the buffer");
    return;
  }
  memcpy(&frame->frame, src, sizeof(page_t));
  frame->is_dirty = true;
  set_LRU_head(frame);
}

void __unpin(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return;
  }

  auto frame = find_frame(table_id, pagenum);
  if (frame == NULL) {
    LOG_WARN("there is no frame in the buffer");
    return;
  }
  frame->pin_count = std::max(frame->pin_count - 1, 0);
}

frame_t *buffer_load_page(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return NULL;
  }

  frame_t *frame = buffer_evict_frame();
  if (frame == NULL) {
    LOG_ERR("failed to evict frame");
    return NULL;
  }
  frame->table_id = table_id;
  frame->page_num = pagenum;
  file_read_page(table_id, pagenum, &frame->frame);

  // push to frame_map
  auto frame_id = std::make_pair(table_id, pagenum);
  frame_map.insert(std::make_pair(frame_id, frame));

  return frame;
}

frame_t *buffer_evict_frame() {
  // find evict page
  auto iter = tail;
  while (iter != NULL) {
    if (iter->pin_count > 0)
      iter = iter->prev;
    else
      break;
  }
  if (iter == NULL) {
    LOG_ERR("all buffer frame is pinned, cannot evict frame");
    return NULL;
  }

  // flush if dirty flag set
  if (iter->is_dirty) {
    file_write_page(iter->table_id, iter->page_num, &iter->frame);
  }

  // remove from frame_map
  auto frame_id = std::make_pair(iter->table_id, iter->page_num);
  frame_map.erase(frame_id);

  // reset frame data
  iter->table_id = -1;
  iter->page_num = 0;
  iter->is_dirty = false;
  iter->pin_count = 0;
  return iter;
}

const page_t *buffer_get_page_ptr(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return NULL;
  }

  auto result = find_frame(table_id, pagenum);
  if (result == NULL) {
    result = buffer_load_page(table_id, pagenum);
    if (result == NULL) {
      LOG_ERR("failed to load page");
      return NULL;
    }
  }

  result->pin_count += 1;
  set_LRU_head(result);

  return &result->frame;
}

frame_t *find_frame(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return NULL;
  }

  auto frame_id = std::make_pair(table_id, pagenum);
  auto search = frame_map.find(frame_id);
  if (search != frame_map.end())
    return search->second;
  else
    return NULL;
}

void set_LRU_head(frame_t *node) {
  if (node == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

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
    LOG_ERR("invalid parameters");
    return;
  }

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
    LOG_ERR("invalid parameters");
    return 1;
  }

  frames = (frame_t *)malloc(num_buf * sizeof(frame_t));
  if (frames == NULL) {
    LOG_ERR("failed to allocate buffer frames");
    return 1;
  }

  head = frames;
  tail = &frames[num_buf - 1];

  // initialize as empty frame and create list
  for (int i = 0; i < num_buf; ++i) {
    frames[i].table_id = -1;
    frames[i].page_num = 0;
    frames[i].is_dirty = false;
    frames[i].pin_count = 0;
    frames[i].next = i + 1 < num_buf ? &frames[i + 1] : NULL;
    frames[i].prev = i - 1 < 0 ? NULL : &frames[i - 1];
  }

  return 0;
}

int free_buffer_manager() {
  // write all dirty frames
  auto iter = head;
  for (auto iter = head; iter != NULL; iter = iter->next) {
    if (iter->is_dirty)
      file_write_page(iter->table_id, iter->page_num, &iter->frame);
  }

  // free resources
  if (frames != NULL) free(frames);
  frame_map.clear();
  return 0;
}

pagenum_t buffer_alloc_page(int64_t table_id) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return 0;
  }

  header_page_t header_page;
  __buffer_read_page(table_id, kHeaderPagenum, &header_page.page);
  if (header_page.header.first_free_page == 0) {
    pagenum_t start = 0, end = 0;
    uint64_t num_new_pages = 0;
    if (file_expand_twice(table_id, &start, &end, &num_new_pages) ||
        start == 0) {
      LOG_ERR("failed to expand file");
      return 0;
    }
    // connect expanded list into header page
    header_page.header.first_free_page = start;
    header_page.header.num_of_pages += num_new_pages;
  }

  auto result = header_page.header.first_free_page;
  page_node_t allocated_page;
  __buffer_read_page(table_id, result, &allocated_page.page);
  header_page.header.first_free_page = allocated_page.next_free_page;
  __unpin(table_id, result);

  __buffer_write_page(table_id, kHeaderPagenum, &header_page.page);
  __unpin(table_id, kHeaderPagenum);
  return result;
}

void buffer_free_page(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0 || pagenum < 1) {
    LOG_ERR("invalid parameters");
    return;
  }

  header_page_t header_page;
  page_node_t page_node;
  __buffer_read_page(table_id, kHeaderPagenum, &header_page.page);
  __buffer_read_page(table_id, pagenum, &page_node.page);

  page_node.next_free_page = header_page.header.first_free_page;
  header_page.header.first_free_page = pagenum;

  __buffer_write_page(table_id, kHeaderPagenum, &header_page.page);
  __buffer_write_page(table_id, pagenum, &page_node.page);
  __unpin(table_id, kHeaderPagenum);
  __unpin(table_id, pagenum);

  auto freed_frame = find_frame(table_id, pagenum);
  if (freed_frame != NULL) set_LRU_tail(freed_frame);
}

void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t *dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

  __buffer_read_page(table_id, pagenum, dest);
}

void buffer_read_header_page(int64_t table_id, header_page_t *dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

  __buffer_read_page(table_id, kHeaderPagenum, (page_t *)dest);
}

void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t *src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

  return __buffer_write_page(table_id, pagenum, src);
}

void buffer_write_header_page(int64_t table_id, const header_page_t *src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

  __buffer_write_page(table_id, kHeaderPagenum, (page_t *)src);
}

void unpin(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return;
  }

  __unpin(table_id, pagenum);
}

void unpin_header(int64_t table_id) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return;
  }

  __unpin(table_id, kHeaderPagenum);
}

int count_free_frames() {
  int result = 0;
  for (auto iter = head; iter != NULL; iter = iter->next) {
    if (iter->pin_count == 0) ++result;
  }
  return result;
}