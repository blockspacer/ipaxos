#ifndef _IPAXOS_UTIL_H_
#define _IPAXOS_UTIL_H_
#include <string>
#include <cstring>
using std::string;
inline bool
bytes_eq(const string& l, const string& r) {
  return (l.length() == r.length() &&
          !std::memcmp(l.c_str(), r.c_str(), l.length()));
}
#endif
