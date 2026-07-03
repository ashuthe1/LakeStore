#pragma once
#include "../types.h"
#include "../options.h"
#include <vector>
#include <string>

namespace lakestore {

class BlockBuilder {
public:
    explicit BlockBuilder(const Options* options);
    
    void Add(const Slice& key, const Slice& value);
    Slice Finish();
    void Reset();
    size_t CurrentSizeEstimate() const;
    bool empty() const { return buffer_.empty(); }

private:
    const Options* options_;
    std::string buffer_;
    std::vector<uint32_t> restarts_;
    int counter_;
    bool finished_;
    std::string last_key_;
};

} // namespace lakestore