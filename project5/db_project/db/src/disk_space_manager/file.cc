#include "disk_space_manager/file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>

#include "log.h"

std::map<std::string, int> table_map;

uint64_t pagenum2offset(pagenum_t pagenum) { return pagenum * kPageSize; }
pagenum_t offset2pagenum(uint64_t offset) { return offset / kPageSize; }

// internal api functions
// to preserve interface , Disk Space Manager uses this functions internally
void __file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR("invalid parameters", pagenum);
    return;
  }
  if (lseek(table_id, pagenum2offset(pagenum), SEEK_SET) < 0) {
    LOG_ERR("cannot seek page %llu", pagenum);
    return;
  }
  if (read(table_id, dest, sizeof(page_t)) < 0) {
    LOG_ERR("cannot read page %llu, errno: %s", pagenum, strerror(errno));
    return;
  }
}

void __file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src,
                       int sync = true) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR("invalid parameters", pagenum);
    return;
  }
  if (lseek(table_id, pagenum2offset(pagenum), SEEK_SET) < 0) {
    LOG_ERR("cannot seek page %llu", pagenum);
    return;
  }
  if (write(table_id, src, sizeof(page_t)) < 0) {
    LOG_ERR("cannot write page %llu", pagenum);
    return;
  }
  if (sync && fsync(table_id) < 0) {
    LOG_ERR("cannot sync write page %llu, errno: %s", pagenum, strerror(errno));
  }
}

void __file_read_header_page(int64_t table_id, header_page_t* dest) {
  if (table_id < 0 || dest == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }
  if (lseek(table_id, pagenum2offset(kHeaderPagenum), SEEK_SET) < 0) {
    LOG_ERR("cannot seek header page");
    return;
  }
  if (read(table_id, dest, sizeof(header_page_t)) < 0) {
    LOG_ERR("cannot read header page");
    return;
  }
}

void __file_write_header_page(int64_t table_id, const header_page_t* src) {
  if (table_id < 0 || src == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }
  if (lseek(table_id, pagenum2offset(kHeaderPagenum), SEEK_SET) < 0) {
    LOG_ERR("cannot seek header page");
    return;
  }
  if (write(table_id, src, sizeof(header_page_t)) < 0) {
    LOG_ERR("cannot write header page");
    return;
  }
  if (fsync(table_id) < 0) {
    LOG_ERR("cannot sync write header page, errno: %s", strerror(errno));
    return;
  }
}

uint64_t __file_size(int64_t table_id) {
  if (table_id < 0) {
    LOG_ERR("invalid parameters");
    return 0;
  }
  auto size = lseek(table_id, 0, SEEK_END);
  if (size < 0) {
    LOG_ERR("cannot seek file end position");
    return 0;
  }
  return size;
}

// utilities used in file.cc (not exported in file.h)
// Expand file by size
void expand(int64_t table_id, uint64_t size) {
  if (size < 1LLU) return;
  if (lseek(table_id, size - 1, SEEK_END) < 0) {
    LOG_ERR("cannot seek file");
    return;
  }
  byte null_byte = 0;
  if (write(table_id, &null_byte, 1) < 0) {
    LOG_ERR("cannot write null byte to expand file");
    return;
  }
  if (lseek(table_id, 0, SEEK_SET) < 0) {
    LOG_WARN("cannot seek to reset");
    return;
  }
  if (fsync(table_id) < 0) {
    LOG_ERR("cannot sync file after expand, errno: %s", strerror(errno));
    return;
  }
}

// Expand file by size and create page list
void expand_and_create_pages(int64_t table_id, uint64_t size, pagenum_t* first,
                             pagenum_t* last, uint64_t* num_new_pages) {
  if (table_id < 0 || size % kPageSize != 0 || size < 1LLU) {
    LOG_ERR("cannot expand database file, wrong parameter");
    return;
  }
  if (first == NULL || last == NULL || num_new_pages == NULL) {
    LOG_ERR("invalid parameters");
    return;
  }

  // expand file
  auto start = __file_size(table_id);
  expand(table_id, size);
  auto end = __file_size(table_id);

  // create page list
  *num_new_pages = (end - start) / kPageSize;

  pagenum_t current_page_num;
  page_node_t page_node;
  for (int64_t i = 0; i < *num_new_pages - 1; ++i) {
    current_page_num = offset2pagenum(start + i * kPageSize);
    page_node.next_free_page = current_page_num + 1;
    __file_write_page(table_id, current_page_num, &page_node.page, false);
  }
  current_page_num = page_node.next_free_page;
  page_node.next_free_page = 0;
  __file_write_page(table_id, current_page_num, &page_node.page);

  *first = offset2pagenum(start);
  *last = current_page_num;
}

// Expand file by size and create page list and connect it to header
void expand_and_create_pages(int64_t table_id, uint64_t size) {
  if (table_id < 0 || size % kPageSize != 0 || size < 1LLU) {
    LOG_ERR("cannot expand database file, wrong parameter");
    return;
  }

  // expand and create pages
  pagenum_t first_page_num, last_page_num;
  uint64_t num_new_pages;
  expand_and_create_pages(table_id, size, &first_page_num, &last_page_num,
                          &num_new_pages);

  // connect created list into header
  header_page_t header_page;
  __file_read_header_page(table_id, &header_page);

  page_node_t page_node;
  __file_read_page(table_id, last_page_num, &page_node.page);
  page_node.next_free_page = header_page.header.first_free_page;
  __file_write_page(table_id, last_page_num, &page_node.page);

  header_page.header.first_free_page = first_page_num;
  header_page.header.num_of_pages += num_new_pages;
  __file_write_header_page(table_id, &header_page);
}

// API

// Open existing database file or create one if not existed.
int64_t file_open_table_file(const char* pathname) {
  if (pathname == NULL) {
    LOG_ERR("invalid parameters");
    return -1;
  }

  // if file is already opened, return its fd
  auto path_str = std::string(pathname);
  auto table_id_found = table_map.find(path_str);
  if (table_id_found != table_map.end()) {
    return table_id_found->second;
  }

  int table_id;
  if (access(pathname, F_OK) != 0) {
    table_id = open(pathname, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (table_id < 0) {
      LOG_ERR("failed to create and open %s, errno: %s", pathname,
              strerror(errno));
      return table_id;
    }
    expand(table_id, kPageSize);  // expand file for header page

    // setup header page
    header_page_t header_page;
    memset(header_page.page.data, 0, kPageSize);
    header_page.header.first_free_page = 0;
    header_page.header.num_of_pages = 1;
    header_page.header.root_page_number = 0;
    __file_write_header_page(table_id, &header_page);

    expand_and_create_pages(table_id, kDefaultFileSize - kPageSize);
  } else {
    table_id = open(pathname, O_RDWR);
    if (table_id < 0) {
      LOG_ERR("failed to open %s, errno: %s", pathname, strerror(errno));
      return table_id;
    }
  }

  // store in descriptors map
  table_map[path_str] = table_id;

  return table_id;
}

int file_expand_twice(int64_t table_id, pagenum_t* start, pagenum_t* end,
                      uint64_t* num_new_pages) {
  if (table_id < 0) {
    LOG_ERR("file descriptor cannot be a negative value");
    return 1;
  }
  expand_and_create_pages(table_id, __file_size(table_id), start, end,
                          num_new_pages);
  return 0;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
  if (table_id < 0) {
    LOG_ERR("file descriptor cannot be a negative value");
    return 0;
  }

  header_page_t header_page;
  __file_read_header_page(table_id, &header_page);
  if (header_page.header.first_free_page == 0) {
    expand_and_create_pages(table_id, __file_size(table_id));
    __file_read_header_page(table_id, &header_page);
  }

  auto pagenum = header_page.header.first_free_page;
  if (pagenum == 0) {
    LOG_ERR("expand database file failed due to some reason");
    return 0;
  }

  page_node_t allocated_page;
  __file_read_page(table_id, pagenum, &allocated_page.page);
  header_page.header.first_free_page = allocated_page.next_free_page;
  __file_write_header_page(table_id, &header_page);

  return pagenum;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
  if (table_id < 0 || pagenum < 1) {
    LOG_ERR("cannot free the page, wrong parameter");
    return;
  }
  header_page_t header_page;
  page_node_t page_node;
  __file_read_header_page(table_id, &header_page);
  __file_read_page(table_id, pagenum, &page_node.page);

  page_node.next_free_page = header_page.header.first_free_page;
  header_page.header.first_free_page = pagenum;
  __file_write_header_page(table_id, &header_page);
  __file_write_page(table_id, pagenum, &page_node.page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest) {
  __file_read_page(table_id, pagenum, dest);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src) {
  __file_write_page(table_id, pagenum, src);
}

// Read an on-disk header page into the in-memory header page structure(dest)
void file_read_header_page(int64_t table_id, header_page_t* dest) {
  __file_read_header_page(table_id, dest);
}

// Write in-memory header page(src) to the on-disk header page
void file_write_header_page(int64_t table_id, const header_page_t* src) {
  __file_write_header_page(table_id, src);
}

uint64_t file_size(int64_t table_id) { return __file_size(table_id); }

// Stop referencing the database file
void file_close_table_files() {
  for (auto table_id_pair : table_map) {
    auto table_id = table_id_pair.second;
    if (table_id > 0) {
      if (close(table_id) < 0) {
        LOG_WARN("failed to close %s, errno: %s", table_id_pair.first.c_str(),
                 strerror(errno));
      }
    }
  }
  table_map.clear();
  sync();
}