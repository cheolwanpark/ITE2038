#ifndef DB_GTEST_PRINTF_H_
#define DB_GTEST_PRINTF_H_

#include <cstdarg>
#include <cstdio>
#include <iostream>

inline void GPRINTF(const char *format, ...) {
  fprintf(stderr, "[          ] [ INFO ] ");
  va_list args;
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fprintf(stderr, "\n");
}

#endif