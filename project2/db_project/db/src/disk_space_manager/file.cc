#include "disk_space_manager/file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <string>

#include "disk_space_manager/endian.h"

std::map<std::string, int> descriptors;

// internal api functions
// to preserve interface , Disk Space Manager uses this functions internally
void __file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
  if (fd < 0 || pagenum < 1 || dest == NULL) return;
  lseek(fd, pagenum, SEEK_SET);
  read(fd, dest, sizeof(page_t));
}

void __file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
  if (fd < 0 || pagenum < 1 || src == NULL) return;
  lseek(fd, pagenum, SEEK_SET);
  write(fd, src, sizeof(page_t));
  fsync(fd);
}

void __file_read_header_page(int fd, header_page_t* dest) {
  if (fd < 0 || dest == NULL) return;
  lseek(fd, 0L, SEEK_SET);
  read(fd, dest, sizeof(header_page_t));
  rev64(dest->header.first_free_page);
  rev64(dest->header.num_of_pages);
}

void __file_write_header_page(int fd, const header_page_t* src) {
  if (fd < 0 || src == NULL) return;
  lseek(fd, 0L, SEEK_SET);
  write(fd, src, sizeof(header_page_t));
  fsync(fd);
}

uint64_t __file_size(int fd) { return lseek(fd, 0L, SEEK_END); }

// utilities used in file.cc (not exported in file.h)
union page_node_t {
  page_t page;
  pagenum_t next_free_page;
};

// Expand file by size
void expand(int fd, uint64_t size) {
  if (size < 1LLU) return;
  lseek(fd, size - 1LLU, SEEK_END);
  byte null_byte = 0;
  write(fd, &null_byte, 1);
  lseek(fd, 0L, SEEK_SET);
  fsync(fd);
}

// Expand file by size and create page list
void expand_and_create_pages(int fd, uint64_t size) {
  if (fd < 0 || size % kPageSize != 0 || size < 1LLU) return;

  // expand file
  auto start = __file_size(fd);
  expand(fd, size);
  auto end = __file_size(fd);

  // create page list
  pagenum_t first_page_num, last_page_num;
  uint64_t num_new_pages = (end - start) / kPageSize;

  pagenum_t current_page_num;
  page_node_t page_node;
  for (int64_t i = 0; i < num_new_pages - 1; ++i) {
    current_page_num = start + i * kPageSize;
    page_node.next_free_page = current_page_num + kPageSize;
    __file_write_page(fd, current_page_num, &page_node.page);
  }
  current_page_num = page_node.next_free_page;
  page_node.next_free_page = 0;
  __file_write_page(fd, current_page_num, &page_node.page);

  first_page_num = start;
  last_page_num = current_page_num;

  // connect created list into header
  header_page_t header_page;
  __file_read_header_page(fd, &header_page);

  __file_read_page(fd, last_page_num, &page_node.page);
  page_node.next_free_page = header_page.header.first_free_page;
  __file_write_page(fd, last_page_num, &page_node.page);

  header_page.header.first_free_page = first_page_num;
  header_page.header.num_of_pages += num_new_pages;
  __file_write_header_page(fd, &header_page);
}

// Open existing database file or create one if not existed.
int file_open_database_file(const char* path) {
  int fd;
  if (access(path, F_OK) != 0) {
    fd = open(path, O_RDWR | O_CREAT);
    expand(fd, kPageSize);  // expand file for header page

    // setup header page
    header_page_t header_page;
    header_page.header.first_free_page = 0;
    header_page.header.num_of_pages = 1;
    __file_write_header_page(fd, &header_page);

    expand_and_create_pages(fd, kDefaultFileSize - kPageSize);
  } else {
    fd = open(path, O_RDWR | O_APPEND);
  }

  // store in descriptors map
  descriptors[std::string(path)] = fd;

  return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
  if (fd < 0) return 0;

  header_page_t header_page;
  __file_read_header_page(fd, &header_page);
  if (header_page.header.first_free_page == 0) {
    expand_and_create_pages(fd, __file_size(fd));
    __file_read_header_page(fd, &header_page);
  }

  auto pagenum = header_page.header.first_free_page;
  if (pagenum == 0) return 0;

  page_node_t allocated_page;
  __file_read_page(fd, pagenum, &allocated_page.page);
  header_page.header.first_free_page = allocated_page.next_free_page;
  __file_write_header_page(fd, &header_page);

  return pagenum;
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
  if (fd < 0 || pagenum < 1) return;
  header_page_t header_page;
  page_node_t page_node;
  __file_read_header_page(fd, &header_page);
  __file_read_page(fd, pagenum, &page_node.page);

  page_node.next_free_page = header_page.header.first_free_page;
  header_page.header.first_free_page = pagenum;
  __file_write_header_page(fd, &header_page);
  __file_write_page(fd, pagenum, &page_node.page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
  __file_read_page(fd, pagenum, dest);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
  __file_write_page(fd, pagenum, src);
}

// Read an on-disk header page into the in-memory header page structure(dest)
void file_read_header_page(int fd, header_page_t* dest) {
  __file_read_header_page(fd, dest);
}

// Write in-memory header page(src) to the on-disk header page
void file_write_header_page(int fd, const header_page_t* src) {
  __file_write_header_page(fd, src);
}

uint64_t file_size(int fd) { return __file_size(fd); }

// Stop referencing the database file
void file_close_database_file() {
  for (auto descriptor_pair : descriptors) {
    auto fd = descriptor_pair.second;
    if (fd > 0) close(fd);
  }
}