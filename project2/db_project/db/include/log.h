#ifndef DB_GTEST_PRINTF_H_
#define DB_GTEST_PRINTF_H_

#include <cstdarg>
#include <cstdio>
#include <iostream>

#include "database.h"

inline void GPRINTF(const char *format, ...) {
  fprintf(stderr, "[          ] [ INFO ] ");
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fprintf(stderr, "\n");
}

inline void __log(bool force_exit, const char *severity,
                  const char *function_name, int line, const char *format,
                  ...) {
  fprintf(stderr, "[%s] [%s] [line %d]: ", severity, function_name, line);
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fprintf(stderr, "\n");

  if (force_exit) {
    shutdown_db();
    exit(-1);
  }
}

#define LOG_ERR(format, ...) \
  __log(true, "Error", __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_WARN(format, ...) \
  __log(false, "Warn", __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...) \
  __log(false, "Info", __func__, __LINE__, format, ##__VA_ARGS__)

#endif