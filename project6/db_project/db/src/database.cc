#include "database.h"

#include <unistd.h>

#include "buffer_manager.h"
#include "disk_space_manager/file.h"
#include "recovery.h"
#include "trx.h"

int init_db(int num_buf, int flag, int log_num, char *log_path,
            char *logmsg_path) {
  if (init_lock_table()) return 1;
  if (init_buffer_manager(num_buf)) return 1;
  if (init_recovery(flag, log_num, log_path, logmsg_path)) return 1;

  // flush frames, logs
  if (buffer_flush_all_frames()) return 1;
  if (flush_log()) return 1;
  return 0;
}

int shutdown_db() {
  free_recovery();
  free_buffer_manager();
  free_lock_table();
  file_close_table_files();
  return 0;
}