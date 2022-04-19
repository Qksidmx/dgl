#include "SubEdgePartitionWithMemTable.h"

namespace skg {
    Status SubEdgePartitionWithMemTable::DeleteVertex(const VertexRequest &request) {
        // first delete in memory
        Status s = m_memTable->DeleteVertex(request);
        if (!s.ok()) { return s; }
        // then delete in disk
        return SubEdgePartition::DeleteVertex(request);
    }

    Status SubEdgePartitionWithMemTable::GetInEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const {
        // first get in memory
        Status s = m_memTable->GetInEdges(req, pQueryResult);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetInEdges(req, pQueryResult);
    }

    Status SubEdgePartitionWithMemTable::GetOutEdges(const VertexRequest &req, EdgesQueryResult *pQueryResult) const {
        // first get in memory
        Status s = m_memTable->GetOutEdges(req, pQueryResult);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetOutEdges(req, pQueryResult);
    }

    Status SubEdgePartitionWithMemTable::GetBothEdges(const VertexRequest &req, EdgesQueryResult *result) const {
        // first get in memory
        Status s = m_memTable->GetBothEdges(req, result);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetBothEdges(req, result);
    }

    Status SubEdgePartitionWithMemTable::GetInVertices(const VertexRequest &req, VertexQueryResult *result) const {
        // first get in memory
        Status s = m_memTable->GetInVertices(req, result);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetInVertices(req, result);
    }

    Status SubEdgePartitionWithMemTable::GetOutVertices(const VertexRequest &req, VertexQueryResult *result) const {
        // first get in memory
        Status s = m_memTable->GetOutVertices(req, result);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetOutVertices(req, result);
    }

    Status SubEdgePartitionWithMemTable::GetBothVertices(const VertexRequest &req, VertexQueryResult *result) const {
        // first get in memory
        Status s = m_memTable->GetBothVertices(req, result);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetBothVertices(req, result);
    }

    Status SubEdgePartitionWithMemTable::GetInDegree(const vid_t dst, int *ans) const {
        // first get in memory
        Status s = m_memTable->GetInDegree(dst, ans);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetInDegree(dst, ans);
    }

    Status SubEdgePartitionWithMemTable::GetOutDegree(const vid_t src, int *ans) const {
        // first get in memory
        Status s = m_memTable->GetOutDegree(src, ans);
        if (!s.ok()) { return s; }
        // then get in disk
        return SubEdgePartition::GetOutDegree(src, ans);
    }

    Status SubEdgePartitionWithMemTable::AddEdge(const EdgeRequest &request) {
        assert(request.GetLabel().edge_label == label().edge_label);
        m_interval.ExtendTo(request.m_dstVid);
        Status s = m_memTable->AddEdge(request);
        return s;
    }

    Status SubEdgePartitionWithMemTable::DeleteEdge(const EdgeRequest &request) {
        assert(request.GetLabel().edge_label == label().edge_label);

        Status s;
        // Trying to delete edge in MemTable
        s = m_memTable->DeleteEdge(request);
        if (s.ok()) { // 已经在 MemTable 中找到边并删除了
            return s;
        } else if (!s.IsNotExist()) {
            // error occur
            return s;
        }

        // Edge is not exist in MemTable. Trying to get edge in disk
        return SubEdgePartition::DeleteEdge(request);
    }

    Status SubEdgePartitionWithMemTable::GetEdgeAttributes(const EdgeRequest &request, EdgesQueryResult *result) {
        assert(request.GetLabel().edge_label == label().edge_label);

        Status s;
        // Trying to get edge attribute in MemTable
        s = m_memTable->GetEdgeAttributes(request, result);
        if (s.ok()) {
            return s;
        } else if (!s.IsNotExist()) {
            // error occur
            return s;
        }

        // Edge is not exist in MemTable. Trying to get in disk
        return SubEdgePartition::GetEdgeAttributes(request, result);
    }

    Status SubEdgePartitionWithMemTable::SetEdgeAttributes(const EdgeRequest &request) {
        // check label
        assert(request.GetLabel().edge_label == label().edge_label);

        metrics::GetInstance()->start_time("SubEdgePartition.SetEdgeAttributes.memtable",metric_duration_type::MILLISECONDS);
        Status s;
        if (m_memTable != nullptr) {
            // Trying to set edge attribute in MemTable
            s = m_memTable->SetEdgeAttributes(request);
            if (s.ok()) {
                metrics::GetInstance()->stop_time("SubEdgePartition.SetEdgeAttributes.memtable");
                return s;
            } else if (!s.IsNotExist()) {
                // error occur
                return s;
            } else { // not exist in mem-table
                // empty
            }
        }
        metrics::GetInstance()->stop_time("SubEdgePartition.SetEdgeAttributes.memtable");

        // Edge is not exist in MemTable. Trying to set edge attribute in disk
        return SubEdgePartition::SetEdgeAttributes(request);
    }

    Status SubEdgePartitionWithMemTable::FlushCache(bool force) {
        // First call SubEdgePartition::Flush,
        // flush origin modifies of loaded edges, attributes on disk
        Status s = SubEdgePartition::FlushCache(force);

        // 不是强制 flush, 且没有 flush 的需求, 则不做处理
        if (!force) {
            return Status::OK();
        }

        return s;
    }

    Status skg::SubEdgePartitionWithMemTable::DeleteEdgeAttrCol(const std::string &columnName) {
        // TODO MemTable 创建属性列
        return SubEdgePartition::DeleteEdgeAttrCol(columnName);
    }

    Status skg::SubEdgePartitionWithMemTable::CreateEdgeAttrCol(ColumnDescriptor descriptor) {
        Status s;
        // FIXME 暂时采取把数据刷入磁盘后, 再调整 memTable 的 Properties
        if (this->m_memTable->GetNumEdges() != 0) {
            s = this->FlushCache(true);
        }
        if (s.ok()) {
            s = m_memTable->CreateEdgeAttrCol(descriptor);
        }
        if (s.ok()) {
            s = SubEdgePartition::CreateEdgeAttrCol(descriptor);
        }
        return s;
    }

    Status skg::SubEdgePartitionWithMemTable::ExportData(
            const std::string &outDir,
            std::shared_ptr<IDEncoder> encoder) {
        Status s = m_memTable->ExportData(outDir, encoder, m_shard_id, m_partition_id);
        if (!s.ok()) { return s; }
        return SubEdgePartition::ExportData(outDir, encoder);
    }

}
