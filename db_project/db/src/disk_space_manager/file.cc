#include "disk_space_manager/file.h"

#include <stdio.h>

#include "disk_space_manager/database_file.h"

// Open existing database file or create one if not existed.
int file_open_database_file(const char* path) {
  DatabaseFile file(path);
  return file.get_fd();
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
  DatabaseFile file(fd);

  header_page_t header_page;
  file.get_header_page(&header_page);
  if (header_page.first_free_page == 0) {
    file.expand_twice();
    file.get_header_page(&header_page);
  }
  auto page_num = header_page.first_free_page;
  if (page_num == 0) return 0;

  page_t allocated_page;
  file.get_page(page_num, &allocated_page);
  header_page.first_free_page = allocated_page.next_free_page;
  file.set_header_page(&header_page);

  return page_num;
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
  DatabaseFile file(fd);

  header_page_t header_page;
  page_t page;
  file.get_header_page(&header_page);
  file.get_page(pagenum, &page);

  page.next_free_page = header_page.first_free_page;
  header_page.first_free_page = pagenum;
  file.set_header_page(&header_page);
  file.set_page(pagenum, &page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
  DatabaseFile file(fd);
  file.get_page(pagenum, dest);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
  DatabaseFile file(fd);
  file.set_page(pagenum, src);
}

// Stop referencing the database file
void file_close_database_file() { DatabaseFile::close_all(); }