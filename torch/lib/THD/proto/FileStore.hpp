#pragma once

#include <map>

#include "Store.hpp"

namespace thd {
namespace proto {

class FileStore : public Store {
 public:
  explicit FileStore(const std::string& path);

  virtual ~FileStore();

  void set(const std::string& name, const std::string& data) override;

  std::string get(const std::string& name) override;

  int64_t add(const std::string& name, int64_t value) override;

  bool check(const std::vector<std::string>& names) override;

  void wait(
      const std::vector<std::string>& names,
      const std::chrono::milliseconds& timeout = kDefaultTimeout) override;

 protected:
  std::string path_;
  int pos_;

  std::map<std::string, std::string> cache_;
};

} // namespace proto
} // namespace thd
