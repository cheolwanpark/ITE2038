#include "database.h"

#include "disk_space_manager/file.h"

int init_db() { return 0; }

int shutdown_db() {
  file_close_table_files();
  return 0;
}