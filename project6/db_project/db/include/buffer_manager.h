#ifndef DB_BUFFER_MANAGER_H_
#define DB_BUFFER_MANAGER_H_

#include <cstdint>

#include "disk_space_manager/file.h"

// initialize buffer manager
// return 0 on success
int init_buffer_manager(int num_buf);

// free buffer manager
int free_buffer_manager();

// allocate new page
// return allocated page number (0 on failed)
pagenum_t buffer_alloc_page(int64_t table_id);

// free page
void buffer_free_page(int64_t table_id, pagenum_t pagenum);

// get frame ptr (if there is no corresponding frame in buffer then load it
// return NULL on failed
page_t *buffer_get_page_ptr(int64_t table_id, pagenum_t pagenum);

// get frame ptr (if there is no corresponding frame in buffer then load it
// return NULL on failed
template <typename T>
T *buffer_get_page_ptr(int64_t table_id, pagenum_t pagenum) {
  return (T *)(buffer_get_page_ptr(table_id, pagenum));
}

// set dirty flag on the frame ptr (gotten by buffer_get_page_ptr)
// if someone putting an invalid ptr, then segfault will occur
void set_dirty(page_t *page);

// set dirty flag on the frame ptr (gotten by buffer_get_page_ptr)
// if someone putting an invalid ptr, then segfault will occur
template <typename T>
void set_dirty(T *page) {
  set_dirty((page_t *)page);
}

// read specific page from the buffer and copy it to dest
// just a wrapper of the buffer_get_page_ptr
// return 0 on success
void buffer_read_page(int64_t table_id, pagenum_t pagenum, page_t *dest);

// read header page from the buffer and copy it to dest
// just a wrapper of the buffer_read_and_copy
void buffer_read_header_page(int64_t table_id, header_page_t *dest);

// write specific page in the buffer
// if buffer does not have that page, failed
// return 0 on success
void buffer_write_page(int64_t table_id, pagenum_t pagenum, const page_t *src);

// write header page in the buffer
// just a wrapper of the buffer_write_page
void buffer_write_header_page(int64_t table_id, const header_page_t *src);

// unpin(decrease pin count) specific page in the buffer
// if buffer does not have that page, failed
void unpin(int64_t table_id, pagenum_t pagenum);

// unpin with frame ptr (gotten by buffer_get_page_ptr)
// if someone putting an invalid ptr, then segfault will occur
void unpin(page_t *page);

// unpin with frame ptr (gotten by buffer_get_page_ptr)
// if someone putting an invalid ptr, then segfault will occur
template <typename T>
void unpin(T *page) {
  unpin((page_t *)page);
}

// unpin(decrease pin count) header page in the buffer
// just a wrapper of unpin
void unpin_header(int64_t table_id);

// for debug purpose
// count not pinned frames
int count_free_frames();

#endif