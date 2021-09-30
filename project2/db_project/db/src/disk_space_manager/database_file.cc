#include "disk_space_manager/database_file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <functional>

#include "disk_space_manager/endian.h"
#include "disk_space_manager/file.h"

DatabaseFile::int_map DatabaseFile::descriptors;

void DatabaseFile::close_all() {
  for (auto int_pair : descriptors) {
    close(int_pair.first);
  }
}

DatabaseFile::DatabaseFile(const char *filename) : _fd(0) {
  if (access(filename, F_OK) != 0) {
    // create new DEFAULT_SIZE file
    _fd = open(filename, O_RDWR | O_CREAT);
    this->expand(kPageSize);

    // setup pages
    header_page_t header_page;
    header_page.first_free_page = 0;
    header_page.num_of_pages = 1;
    this->set_header_page(&header_page);

    expand_pages(kDefaultFileSize - kPageSize);

    this->get_header_page(&header_page);
  } else {
    _fd = open(filename, O_RDWR | O_APPEND);
  }

  // store in descriptors map
  descriptors[_fd] = true;
}

DatabaseFile::DatabaseFile(int fd) : _fd(fd) { ; }

void DatabaseFile::get_header_page(header_page_t *page) const {
  lseek(_fd, 0L, SEEK_SET);
  read(_fd, page, sizeof(header_page_t));
  rev64(page->first_free_page);
  rev64(page->num_of_pages);
}

void DatabaseFile::set_header_page(const header_page_t *page) {
  lseek(_fd, 0L, SEEK_SET);
  write(_fd, page, sizeof(header_page_t));
  fsync(_fd);
}

void DatabaseFile::get_page(pagenum_t id, page_t *page) const {
  lseek(_fd, id, SEEK_SET);
  read(_fd, page, sizeof(page_t));
}

void DatabaseFile::set_page(pagenum_t id, const page_t *page, bool sync) {
  lseek(_fd, id, SEEK_SET);
  write(_fd, page, sizeof(page_t));
  if (sync) fsync(_fd);
}

pagenum_t DatabaseFile::pop_free_page() {
  header_page_t header_page;
  this->get_header_page(&header_page);
  if (header_page.first_free_page == 0) {
    this->expand_twice();
    this->get_header_page(&header_page);
  }
  auto page_num = header_page.first_free_page;
  if (page_num == 0) return 0;

  page_list_node_t allocated_page;
  this->get_page_list_node(page_num, &allocated_page);
  header_page.first_free_page = allocated_page.next_free_page;
  this->set_header_page(&header_page);

  return page_num;
}

void DatabaseFile::push_free_page(pagenum_t id) {
  header_page_t header_page;
  page_list_node_t page;
  this->get_header_page(&header_page);
  this->get_page_list_node(id, &page);

  page.next_free_page = header_page.first_free_page;
  header_page.first_free_page = id;
  this->set_header_page(&header_page);
  this->set_page_list_node(id, &page);
}

void DatabaseFile::expand_twice() { this->expand_pages(this->get_size()); }

void DatabaseFile::close_file() {
  if (_fd != 0) {
    close(_fd);
    descriptors.erase(_fd);
    _fd = 0;
  }
}

int DatabaseFile::get_fd() const { return _fd; }

uint64_t DatabaseFile::get_size() const { return lseek(_fd, 0L, SEEK_END); }

void DatabaseFile::get_page_list_node(pagenum_t id,
                                      page_list_node_t *page) const {
  lseek(_fd, id, SEEK_SET);
  read(_fd, page, sizeof(page_list_node_t));
  rev64(page->next_free_page);
}

void DatabaseFile::set_page_list_node(pagenum_t id,
                                      const page_list_node_t *page, bool sync) {
  lseek(_fd, id, SEEK_SET);
  write(_fd, page, sizeof(page_list_node_t));
  if (sync) fsync(_fd);
}

void DatabaseFile::create_free_pages(uint64_t start, uint64_t end,
                                     pagenum_t *first_page_num,
                                     pagenum_t *last_page_num,
                                     uint64_t *num_new_pages) {
  if (start > end || ((end - start) % kPageSize != 0)) return;

  uint64_t num_of_pages = (end - start) / kPageSize;

  pagenum_t current_page_num;
  page_list_node_t page;
  for (int64_t i = 0; i < num_of_pages - 1; ++i) {
    current_page_num = start + i * kPageSize;
    page.next_free_page = current_page_num + kPageSize;
    this->set_page_list_node(current_page_num, &page, false);
  }
  current_page_num = page.next_free_page;
  page.next_free_page = 0;
  this->set_page_list_node(current_page_num, &page, false);

  fsync(_fd);

  *first_page_num = start;
  *last_page_num = current_page_num;
  *num_new_pages = num_of_pages;
}

void DatabaseFile::expand(uint64_t size) {
  if (size < 1LLU) return;
  lseek(_fd, size - 1LLU, SEEK_END);
  byte null_byte = 0;
  write(_fd, &null_byte, 1);
  lseek(_fd, 0L, SEEK_SET);
  fsync(_fd);
}

void DatabaseFile::expand_pages(uint64_t size) {
  if (size % kPageSize != 0 || size < 1LLU) return;
  auto start = this->get_size();
  this->expand(size);
  auto end = this->get_size();

  header_page_t header_page;
  this->get_header_page(&header_page);

  pagenum_t first_page_num, last_page_num;
  uint64_t num_new_pages;
  this->create_free_pages(start, end, &first_page_num, &last_page_num,
                          &num_new_pages);

  page_list_node_t last_page;
  this->get_page_list_node(last_page_num, &last_page);
  last_page.next_free_page = header_page.first_free_page;
  this->set_page_list_node(last_page_num, &last_page);

  header_page.first_free_page = first_page_num;
  header_page.num_of_pages += num_new_pages;
  this->set_header_page(&header_page);
}