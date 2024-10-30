#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <stdint.h>

#include <climits>

typedef uint64_t pagenum_t;
typedef char byte;

const uint64_t kPageSize = 4 * 1024;
const uint64_t kDefaultFileSize = 10 * 1024 * 1024;
const pagenum_t kHeaderPagenum = 0;
const pagenum_t kNullPagenum = ULONG_MAX;

struct page_t {
  byte data[kPageSize];
};

union header_page_t {
  page_t page;
  struct {
    pagenum_t first_free_page;
    uint64_t num_of_pages;
    pagenum_t root_page_number;
  } header;
};

union page_node_t {
  page_t page;
  pagenum_t next_free_page;
};

// Open existing database file or create one if not existed.
int64_t file_open_table_file(const char *pathname);

// Open existing database file if it is not loaded.
int64_t file_open_table_file(int64_t table_id);

// Expand file twice and create new free pages
// for page allocation in buffer layer
// return 0 on success
int file_expand_twice(int64_t table_id, pagenum_t *start, pagenum_t *end,
                      uint64_t *num_new_pages);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t *dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t *src,
                     int sync = true);

// Read an on-disk header page into the in-memory header page structure(dest)
void file_read_header_page(int64_t table_id, header_page_t *dest);

// Write in-memory header page(src) to the on-disk header page
void file_write_header_page(int64_t table_id, const header_page_t *src,
                            int sync = true);

// Calculate file size (byte)
uint64_t file_size(int64_t table_id);

// sync file descriptor
void file_sync_all();

// Stop referencing the database file
void file_close_table_files();

#endif