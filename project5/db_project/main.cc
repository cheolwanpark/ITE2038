#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "database.h"
#include "disk_space_manager/file.h"
#include "index_manager/index.h"

const int DUMMY_TRX = -1;
const int N = 4;
long long keys[N] = {
    4567,
    3456,
    2345,
    1234,
};

int sizes[N] = {
    50,
    60,
    70,
    80,
};

char data[4][112] = {
    "Hello World!",
    "My Name is Cheolwan Park",
    "This is test db file generation code!",
    "Bye Bye!",
};

void check_test_file() {
  init_db(100);
  auto t1 = file_open_table_file("test.db");

  char read_buf[112];
  unsigned short size;
  for (int i = 0; i < N; ++i) {
    memset(read_buf, 0, sizeof(read_buf));
    if (db_find(t1, keys[i], read_buf, &size, DUMMY_TRX)) {
      printf("failed to find!");
      exit(-1);
    }
    if (strcmp(read_buf, data[i]) != 0) {
      printf("data differs!");
      exit(-1);
    }
    if (size != sizes[i]) {
      printf("size differs!");
      exit(-1);
    }
  }
  shutdown_db();
  printf("success\n");
}

int main(int argc, char** argv) {
  // generate_test_file();
  check_test_file();
  return 0;
}
