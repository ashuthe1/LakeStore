#include "types.h"

namespace lakestore {

std::string InternalKey::Encode() const {
    std::string result = user_key;
    // Pack sequence_number and type into 8 bytes (little-endian semantics)
    uint64_t packed = (seq << 8) | type;
    char buf[8];
    // Fast binary copy
    std::memcpy(buf, &packed, 8); 
    result.append(buf, 8);
    return result;
}

InternalKey InternalKey::Decode(const Slice& s) {
    if (s.size < 8) {
        throw std::invalid_argument("Slice too small to decode InternalKey");
    }
    
    InternalKey ik;
    ik.user_key = std::string(s.data, s.size - 8);
    
    uint64_t packed;
    std::memcpy(&packed, s.data + s.size - 8, 8);
    
    ik.seq = packed >> 8;
    ik.type = static_cast<ValueType>(packed & 0xFF);
    
    return ik;
}

} // namespace lakestore