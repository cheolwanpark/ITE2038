#include "database.h"

#include <unistd.h>

#include "buffer_manager.h"
#include "disk_space_manager/file.h"

int init_db(int num_buf) {
  init_buffer_manager(num_buf);
  return 0;
}

int shutdown_db() {
  file_close_table_files();
  return 0;
}