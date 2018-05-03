#include "FileStore.hpp"

#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <chrono>
#include <iostream>
#include <sstream>
#include <system_error>
#include <thread>

#define SYSASSERT(rv) \
  if ((rv) < 0) throw std::system_error(errno, std::system_category());

#define SYSCHECK(expr) {                                                \
    errno = 0; auto ___output = (expr); (void)___output;                \
    SYSASSERT(errno);                                                   \
  }

namespace thd {
namespace proto {

namespace {

// For a comprehensive overview of file locking methods,
// see: https://gavv.github.io/blog/file-locks/.
// We stick to flock(2) here because we don't care about
// locking byte ranges and don't want locks to be process-wide.

// RAII wrapper around flock(2)
class Lock {
 public:
  explicit Lock(int fd, int operation) : fd_(fd) {
    flock(operation);
  }

  ~Lock() {
    if (fd_ >= 0) {
      flock(LOCK_UN);
    }
  }

  Lock(const Lock& that) = delete;

  Lock(Lock&& other) noexcept {
    fd_ = other.fd_;
    other.fd_ = -1;
  }

 protected:
  int fd_;

  void flock(int operation) {
    while (true) {
      auto rv = ::flock(fd_, operation);
      if (rv == -1) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(), "flock");
      }
      break;
    }
  }
};

class File {
 public:
  explicit File(const std::string& path, int flags) {
    while (true) {
      fd_ = ::open(path.c_str(), flags, 0644);
      if (fd_ < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(
            errno,
            std::generic_category(),
            "Unable to open " + path);
      }
      break;
    }
  }

  ~File() {
    ::close(fd_);
  }

  Lock lockShared() {
    return Lock(fd_, LOCK_SH);
  }

  Lock lockExclusive() {
    return Lock(fd_, LOCK_EX);
  }

  off_t seek(off_t offset, int whence) {
    auto rv = lseek(fd_, offset, whence);
    SYSASSERT(rv);
    return rv;
  }

  off_t tell() {
    auto rv = lseek(fd_, 0, SEEK_CUR);
    SYSASSERT(rv);
    return rv;
  }

  off_t size() {
    auto pos = tell();
    auto size = seek(0, SEEK_END);
    seek(pos, SEEK_SET);
    return size;
  }

  void writeString(const std::string& str) {
    uint32_t len = str.size();
    SYSCHECK(write(fd_, &len, sizeof(len)));
    SYSCHECK(write(fd_, str.c_str(), len));
  }

  void readString(std::string& str) {
    uint32_t len;
    SYSCHECK(read(fd_, &len, sizeof(len)));
    std::vector<char> buf(len);
    SYSCHECK(read(fd_, buf.data(), len));
    str.assign(buf.begin(), buf.end());
  }

 protected:
  int fd_;
};

} // namespace

FileStore::FileStore(const std::string& path)
    : Store(),
      path_(path),
      pos_(0) {
}

FileStore::~FileStore() {
}

void FileStore::set(const std::string& name, const std::string& data) {
  File file(path_, O_RDWR | O_CREAT);
  auto lock = file.lockExclusive();
  file.seek(0, SEEK_END);
  file.writeString(name);
  file.writeString(data);
}

std::string FileStore::get(const std::string& key) {
  while (cache_.count(key) == 0) {
    File file(path_, O_RDONLY);
    auto lock = file.lockShared();
    auto size = file.size();
    if (size == pos_) {
      // No new entries; sleep for a bit
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    std::string tmpKey;
    std::string tmpValue;
    file.seek(pos_, SEEK_SET);
    while (size > pos_) {
      file.readString(tmpKey);
      file.readString(tmpValue);
      cache_[tmpKey] = tmpValue;
      pos_ = file.tell();
    }
  }

  return cache_[key];
}

int64_t FileStore::add(const std::string& key, int64_t i) {
  File file(path_, O_RDWR | O_CREAT);
  auto lock = file.lockExclusive();
  auto size = file.size();
  if (size > pos_) {
    // New entries; update cache since this key may have been updated
    std::string tmpKey;
    std::string tmpValue;
    file.seek(pos_, SEEK_SET);
    while (size > pos_) {
      file.readString(tmpKey);
      file.readString(tmpValue);
      cache_[tmpKey] = tmpValue;
      pos_ = file.tell();
    }
  }

  int64_t ti = 0;
  std::stringstream sin(cache_[key]);
  sin >> ti;
  ti += i;

  // File cursor is at the end of the file now, and we have an
  // exclusive lock, so we can write the new value.
  std::stringstream sout;
  sout << ti;
  file.writeString(key);
  file.writeString(sout.str());

  return ti;
}

bool FileStore::check(const std::vector<std::string>& keys) {
  File file(path_, O_RDONLY);
  auto lock = file.lockShared();
  auto size = file.size();
  if (size != pos_) {
    std::string tmpKey;
    std::string tmpValue;
    file.seek(pos_, SEEK_SET);
    while (size > pos_) {
      file.readString(tmpKey);
      file.readString(tmpValue);
      cache_[tmpKey] = tmpValue;
      pos_ = file.tell();
    }
  }

  for (const auto& key : keys) {
    if (cache_.count(key) == 0) {
      return false;
    }
  }

  return true;
}

void FileStore::wait(
    const std::vector<std::string>& names,
    const std::chrono::milliseconds& timeout) {
  // Not using inotify because it doesn't work on many
  // shared filesystems (such as NFS).
  const auto start = std::chrono::steady_clock::now();
  while (!check(names)) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - start);
    if (timeout != kNoTimeout && elapsed > timeout) {
      throw std::runtime_error("Wait timeout");
    }

    /* sleep override */
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

}
}
