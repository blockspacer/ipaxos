#ifndef _IPAXOS_UTIL_H_
#define _IPAXOS_UTIL_H_
#include <string>
#include <cstring>
#include <vector>
#include <map>
using std::string;
inline bool
bytes_eq(const string& l, const string& r) {
  return (l.length() == r.length() &&
          !std::memcmp(l.c_str(), r.c_str(), l.length()));
}

template<typename T, typename U>
std::pair<std::vector<uint64_t>, std::vector<uint64_t>>
diff_ranges(T a,
            U b) {
  auto diffs = std::make_pair(std::vector<uint64_t>(), std::vector<uint64_t>());
  std::vector<uint64_t> ranges;
  if (a.size() == 0) {
    diffs.second = std::move(b);
    return diffs;
  }
  
  if (b.size() == 0) {
    std::move(a.begin(), a.end(), std::back_inserter(diffs.first));
    return diffs;
  }

  unsigned itera = 0;
  unsigned iterb = 0;
  while (itera != a.size() &&
      iterb != b.size()) {
    auto& lo_a = a[itera];
    auto& hi_a = a[itera + 1];
    auto& lo_b = b[iterb];
    auto& hi_b = b[iterb + 1];
    if (hi_a < lo_b) {
      diffs.first.push_back(lo_a);
      diffs.first.push_back(hi_a);
      itera += 2;
    } else if (hi_a < hi_b) {
      if (lo_a < lo_b) {
        diffs.first.push_back(lo_a);
        diffs.first.push_back(lo_b - 1);
      } else if (lo_a > lo_b) {
        diffs.second.push_back(lo_b);
        diffs.second.push_back(lo_a - 1);
      }
      itera += 2;
      lo_b = hi_a + 1;
    } else if (hi_a == hi_b) {
      if (lo_a < lo_b) {
        diffs.first.push_back(lo_a);
        diffs.first.push_back(lo_b - 1);
      } else if (lo_a > lo_b) {
        diffs.second.push_back(lo_b);
        diffs.second.push_back(lo_a - 1);
      }
      iterb += 2;
      itera += 2;
    } else {
      if (lo_a < lo_b) {
        diffs.first.push_back(lo_a);
        diffs.first.push_back(lo_b - 1);
      } else if (lo_a == lo_b) {}
      else if (lo_a <= hi_b) {
        diffs.second.push_back(lo_b);
        diffs.second.push_back(lo_a - 1);
        lo_a = hi_b + 1;
      } else {
        diffs.second.push_back(lo_b);
        diffs.second.push_back(hi_b);
      }
      iterb += 2;
      lo_a = hi_b + 1;
    }
  }
  if (itera != a.size()) {
    while (itera != a.size()) {
      diffs.first.push_back(a[itera]);
      diffs.first.push_back(a[itera + 1]);
      itera += 2;
    }
  }

  if (iterb != b.size()) {
    while (iterb != b.size()) {
      diffs.first.push_back(b[iterb]);
      diffs.first.push_back(b[iterb + 1]);
      iterb += 2;
    }
  }

  return diffs;
}
#endif
