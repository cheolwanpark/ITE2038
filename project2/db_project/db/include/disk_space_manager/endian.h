#ifndef DBMS_ENDIAN_H
#define DBMS_ENDIAN_H

#include <cstdint>

inline uint16_t reverse16(uint16_t value) {
  union {
    uint16_t val;
    uint8_t b[2];
  } u;
  u.val = value;
  return ((u.b[1] << 8) | u.b[0]);
}

inline void rev16(uint16_t& v) { v = reverse16(v); }

inline uint32_t reverse32(uint32_t value) {
  union {
    uint32_t val;
    uint8_t b[4];
  } u;
  u.val = value;
  return ((u.b[3] << 24) | (u.b[2] << 16) | (u.b[1] << 8) | u.b[0]);
}

inline void rev32(uint32_t& v) { v = reverse32(v); }

inline uint32_t reverse64(uint64_t value) {
  union {
    uint32_t val;
    uint8_t b[8];
  } u;
  u.val = value;
  return (((uint64_t)u.b[7] << 56) | ((uint64_t)u.b[6] << 48) |
          ((uint64_t)u.b[5] << 40) | ((uint64_t)u.b[4] << 32) | (u.b[3] << 24) |
          (u.b[2] << 16) | (u.b[1] << 8) | u.b[0]);
}

inline void rev64(uint64_t& v) { v = reverse32(v); }

#endif