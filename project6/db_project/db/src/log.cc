#include "log.h"

#include <pthread.h>

#ifdef THREAD_ALIGN
pthread_mutex_t debug_log_latch = PTHREAD_MUTEX_INITIALIZER;
#endif

void GPRINTF(const char *format, ...) {
#ifdef THREAD_ALIGN
  pthread_mutex_lock(&debug_log_latch);
#endif
  fprintf(stderr, "[          ] [ INFO ] ");
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fprintf(stderr, "\n");
#ifdef THREAD_ALIGN
  pthread_mutex_unlock(&debug_log_latch);
#endif
}

void __log(bool force_exit, const char *severity, const char *function_name,
           int line, const char *format, ...) {
#ifdef THREAD_ALIGN
  pthread_mutex_lock(&debug_log_latch);
#endif
  fprintf(stderr, "[%s] [%s] [line %d]: ", severity, function_name, line);
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fprintf(stderr, "\n");
#ifdef THREAD_ALIGN
  pthread_mutex_unlock(&debug_log_latch);
#endif
  if (force_exit) {
    shutdown_db();
    exit(-1);
  }
}