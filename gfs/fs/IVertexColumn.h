#ifndef STARKNOWLEDGEGRAPHDATABASE_IVERTEXCOLUMN_H
#define STARKNOWLEDGEGRAPHDATABASE_IVERTEXCOLUMN_H

#include <string>
#include <memory>

#include "util/types.h"
#include "util/status.h"
#include "ColumnDescriptor.h"
#include "ResultProperties.h"

#include "util/skgfilenames.h"
#include "util/pathutils.h"

namespace skg {
class IVertexColumn {
public:
    /**
     * @brief 创建节点属性列
     * @param storageDir
     * @param label         节点的 label
     * @param descriptor    属性列名字 / 属性占用的字节空间
     * @param capacity      节点容量(需要至少存多少个节点)
     * @return
     */
    static
    Status CreateWithCapacity(
            const std::string &storageDir,
            const std::string &label, const ColumnDescriptor &descriptor,
            const vid_t capacity);

    /**
     * @brief 打开节点属性列操作句柄
     * @param storageDir
     * @param capacity
     * @param label
     * @param descriptor
     * @param s
     * @return
     */
    static
    std::shared_ptr<IVertexColumn> OpenColumn(
            const std::string &storageDir,
            const std::string &label, const ColumnDescriptor &descriptor,
            Status *s);
public:
    IVertexColumn(const ColumnDescriptor &desc);

    virtual ~IVertexColumn() = default;

    /**
     * 打开操作节点属性列
     */
    virtual
    Status Open() = 0;

    /**
     * 关闭, flush 修改
     */
    virtual
    Status Close() = 0;

    virtual
    Status Drop() = 0;

    /**
     * flush 修改
     */
    virtual
    Status Flush() = 0;

    /**
     * @brief 确保有足够的空间存储 storageCapacity 个节点的属性
     * @param storageCapacity 最大存储节点数
     * @return
     */
    virtual
    Status EnsureStorage(const vid_t storageCapacity) = 0;

    /**
     * @brief 获取节点属性值
     * @param vtx   节点 id
     * @param value 属性值
     * @return
     */
    virtual
    Status Get(const vid_t vtx, ResultProperties *value, size_t offset) const = 0;

    /**
     * @brief 更新节点属性值
     * @param vtx
     * @param value
     * @return
     */
    virtual
    Status Set(const vid_t vtx, const Slice &value) = 0;

    inline
    const char *name() const {
        return m_name.c_str();
    }

    virtual
    const char *filename() const = 0;

    virtual ColumnType vertexColType() const = 0;

    virtual
    size_t value_size() const  = 0;

    inline
    int32_t id() const {
        return m_id;
    }

private:
    // 列名称
    std::string m_name;
    // 列 id
    int32_t m_id;
};

typedef std::shared_ptr<IVertexColumn> IVertexColumnPtr;

}

#endif //STARKNOWLEDGEGRAPHDATABASE_IVERTEXCOLUMN_H
