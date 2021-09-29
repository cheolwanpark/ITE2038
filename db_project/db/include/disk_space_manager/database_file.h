#ifndef DB_DATABASE_FILE_H_
#define DB_DATABASE_FILE_H_

#include <stdint.h>
#include <stdio.h>

#include <map>

#include "file.h"

const uint64_t kDefaultFileSize = 10 * 1024 * 1024;
const uint64_t kHeaderPageDataSize =
    kPageSize - sizeof(pagenum_t) - sizeof(uint64_t);

struct header_page_t {
  pagenum_t first_free_page;
  uint64_t num_of_pages;
  byte data[kHeaderPageDataSize];
};

class DatabaseFile {
 private:
  typedef std::map<int, int> int_map;
  static int_map descriptors;

 public:
  static void close_all();

 private:
  int _fd;

 public:
  DatabaseFile(const char *filename);

  DatabaseFile(int fd);

  void get_header_page(header_page_t *page) const;

  void set_header_page(const header_page_t *page);

  void get_page(pagenum_t id, page_t *page) const;

  void set_page(pagenum_t id, const page_t *page);

  void expand_twice();

  void close_file();

  int get_fd() const;

  uint64_t get_size() const;

 private:
  void create_free_pages(uint64_t start, uint64_t end, pagenum_t *fist_page_num,
                         pagenum_t *last_page_num, uint64_t *num_new_pages);
  void expand(uint64_t size);

  void expand_pages(uint64_t size);
};

#endif