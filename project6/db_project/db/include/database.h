#ifndef DB_DB_H_
#define DB_DB_H_

int init_db(int num_buf, int flag, int log_num, char *log_path,
            char *logmsg_path);

int shutdown_db();

#endif