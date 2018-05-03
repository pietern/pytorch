#include "Store.hpp"

namespace thd {
namespace proto {

constexpr std::chrono::milliseconds Store::kDefaultTimeout;
constexpr std::chrono::milliseconds Store::kNoTimeout;

// Define destructor symbol for abstract base class.
Store::~Store() {
}

} // namespace proto
} // namespace thd
