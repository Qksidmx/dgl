#ifndef STARKNOWLEDGEGRAPHDATABASE_RESULTPROPERTIES_H
#define STARKNOWLEDGEGRAPHDATABASE_RESULTPROPERTIES_H

#include <limits>

#include "util/types.h"
#include "util/status.h"
#include "ColumnDescriptor.h"

#include "util/slice.h"

namespace skg {
class ResultProperties {
public:
    friend class RequestUtilities;
private:
    /**
     * 属性数据.
     * 定长字段, 如 int, float, fixed-bytes 等, 直接存储内容
     * 变长字段, 则存储其在 m_var_results 中的初始偏移量 (size_t)
     */
    Bytes m_fixed_bytes;
    /**
     * 用于存储变长属性数据
     */
    Bytes m_var_bytes;

    // 属性 bitset
    PropertiesBitset_t m_bitset;

public:
    ResultProperties(size_t fixed_bytes_len)
            : m_fixed_bytes(fixed_bytes_len, 0), m_var_bytes(), m_bitset() {
    }

    void resize_fixed_bytes(size_t fixed_bytes_len) {
        m_fixed_bytes.resize(fixed_bytes_len);
    }

    inline bool is_null(int32_t id) const {
        return !m_bitset.IsPropertySet(id);
    }

    inline void set(int32_t id) {
        m_bitset.SetProperty(id);
    }

    inline const PropertiesBitset_t& bitset() const {
        return m_bitset;
    }

    Slice fixed_bytes() const {
        return Slice(m_fixed_bytes.data(), m_fixed_bytes.size());
    }

    size_t fixed_bytes_length() const {
        return m_fixed_bytes.size();
    }

    Slice var_bytes() const {
        return Slice(m_var_bytes.data(), m_var_bytes.size());
    }

    size_t var_bytes_length() const {
        return m_var_bytes.size();
    }

    /**
     * @brief 设置定长属性. 如 int32_t, int64_t, float, double
     * @tparam T
     * @param v
     * @param offset
     * @return
     */
    template <typename T>
    Status put(T v, size_t offset, int32_t id) {
        // https://en.cppreference.com/w/cpp/types/is_arithmetic
        static_assert(std::is_arithmetic<T>::value,
                      "only  an integral type or a floating-point type allow");
        if (m_fixed_bytes.size() >= offset + sizeof(v)) {
            memcpy(m_fixed_bytes.data() + offset, &v, sizeof(v));
            m_bitset.SetProperty(id);
            return Status::OK();
        } else {
            return Status::InvalidArgument(); // 超出容纳的范围
        }
    }

    Status putBytes(const std::string &v, size_t offset, int32_t id) {
        return putBytes(Slice(v), offset, id);
    }

    /**
     * @brief 设置定长的 fix-bytes 属性
     * @param s
     * @param offset
     * @return
     */
    Status putBytes(Slice s, size_t offset, int32_t id) {
        if (id == ColumnDescriptor::ID_VERTICES_BITSET) {
            memcpy(m_bitset.m_bitset, s.data(), s.size());
            return Status::OK();
        }
        if (m_fixed_bytes.size() >= offset + s.size()) {
            memcpy(m_fixed_bytes.data() + offset, s.data(), s.size());
            m_bitset.SetProperty(id);
            return Status::OK();
        } else {
            return Status::InvalidArgument(); // 超出容纳的范围
        }
    }

    /**
     * @brief 获取定长属性值. int32_t, int64_t, float, double
     * @tparam T
     * @param offset
     * @return
     */
    template <typename T>
    T get(size_t offset) const {
        assert(offset < m_fixed_bytes.size());
        return *reinterpret_cast<const T *>(m_fixed_bytes.data() + offset);
    }

    /**
     * @brief 获取定长属性值 fix-bytes
     * @param beg_
     * @param end_
     * @param remove_suffix_zeros true 则去除末尾 '\x00'
     * @return
     */
    Slice get(size_t beg_, size_t end_, bool remove_suffix_zeros=false) const {
        assert(beg_ <= end_);
        assert(beg_ <= m_fixed_bytes.size());
        assert(end_ <= m_fixed_bytes.size());
        Slice s(m_fixed_bytes.data() + beg_, end_ - beg_);
        if (remove_suffix_zeros) {
            while (!s.empty() && s[s.size() - 1] == '\x00') { s.remove_suffix(1); }
        }
        return s;
    }


    /**
     * @brief 设置变长属性
     * @param slice
     * @param offset
     * @return
     */
    Status putVar(Slice slice, size_t offset, int32_t id) {
        if (m_var_bytes.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument(); // 超出容纳的范围
        }
        uint32_t offset_to_var = static_cast<uint32_t>(m_var_bytes.size());
        Status s = this->put(offset_to_var, offset, id);
        if (s.ok()) {
            // 函数开始保证不能放超过 uint32_t 长度的字符串
            uint32_t var_len = slice.size();
            m_var_bytes.insert(m_var_bytes.end(), (const char *)(&var_len), (const char *)(&var_len) + sizeof(var_len));
            m_var_bytes.insert(m_var_bytes.end(), slice.data(), slice.data() + slice.size());
        }
        return s;
    }

    /**
     * @brief 获取变长属性
     * @param offset
     * @return
     */
    Slice getVar(size_t offset) const {
        assert(offset <= m_fixed_bytes.size() - sizeof(uint32_t));
        const uint32_t offset_to_var = get<uint32_t>(offset);
        const uint32_t var_len = *reinterpret_cast<const uint32_t*>(m_var_bytes.data() + offset_to_var);
        return Slice(m_var_bytes.data() + offset_to_var + sizeof(var_len), var_len);
    }

    void clear() {
        m_fixed_bytes.clear();
        m_var_bytes.clear();
    }

    void assign_fix(const Slice &s) {
        m_fixed_bytes.assign(s.data(), s.data() + s.size());
    }

    void assign_var(const Slice &s) {
        m_var_bytes.assign(s.data(), s.data() + s.size());
    }

    // 复制/赋值
    ResultProperties(const ResultProperties &rhs)
            : m_fixed_bytes(rhs.m_fixed_bytes),
              m_var_bytes(rhs.m_var_bytes),
              m_bitset(rhs.m_bitset) {
    }
    ResultProperties& operator=(const ResultProperties&rhs) {
        if (this != &rhs) {
            m_fixed_bytes = rhs.m_fixed_bytes;
            m_var_bytes = rhs.m_var_bytes;
            m_bitset = rhs.m_bitset;
        }
        return *this;
    }

    // 移动
    ResultProperties(ResultProperties &&rhs) noexcept {
        *this = std::move(rhs);
    }
    ResultProperties& operator=(ResultProperties &&rhs) noexcept {
        if (this != &rhs) {
            m_fixed_bytes.swap(rhs.m_fixed_bytes);
            m_var_bytes.swap(rhs.m_var_bytes);
            m_bitset = rhs.m_bitset;
        }
        return *this;
    }

};
}

#endif //STARKNOWLEDGEGRAPHDATABASE_RESULTPROPERTIES_H
