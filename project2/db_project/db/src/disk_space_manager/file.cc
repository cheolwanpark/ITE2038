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
  return file.pop_free_page();
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
  DatabaseFile file(fd);
  file.push_free_page(pagenum);
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