#ifndef DB_GTEST_PRINTF_H_
#define DB_GTEST_PRINTF_H_

#include <cstdarg>
#include <cstdio>
#include <iostream>

#include "database.h"

#define THREAD_ALIGN

void GPRINTF(const char *format, ...);

void __log(int exitcode, const char *severity, const char *function_name,
           int line, const char *format, ...);

#define LOG_ERR(code, format, ...) \
  __log(code, "Error", __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_WARN(format, ...) \
  __log(0, "Warn", __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...) \
  __log(0, "Info", __func__, __LINE__, format, ##__VA_ARGS__)

#endif