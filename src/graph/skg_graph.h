#ifndef _H_SKG_GRAPH
#define _H_SKG_GRAPH
#include <future>
#include <thread>
#include <algorithm>
#include "fmt/format.h"
#include "fmt/time.h"
#include "env/env.h"

#include "util/types.h"
#include "fs/skgfs.h"

#include "util/cmdopts.h"
#include "util/skglogger.h"


#include "metrics/metrics.hpp"
#include "metrics/reps/file_reporter.hpp"
#include "metrics/reps/basic_reporter.hpp"


#include "fs/SubEdgePartition.h"
#include "fs/VertexColumnList.h"
#include "fs/ShardTree.h"
//
//for dgl integration
#include "../c_api_common.h"
#include <dgl/graph.h>
#include <stdio.h>

using namespace skg;
using namespace dgl;

class SkgGraph{
	public:
	    SkgGraph()
	    {
                this->options.LoadOptions();
                this->options.force_create = true;
                std::string dbName = "default";
                this->db_dir = options.GetDBDir(dbName);
	        s = PathUtils::CreateDirIfMissing(this->db_dir);
		if (s.ok()) 
	        {
			if (options.force_create) 
			{
			    // FIXME
			    SKG_LOG_WARNING("force_create is true, going to drop old database @ ..", db_dir);
			    s = Env::Default()->DeleteDir(db_dir, true, true);
			    if (s.ok()) 
			    {
				s = Status::OK();
			    }
			} 
			else 
			{
			    // 已有数据存在, 不可静态建表
			    s = Status::InvalidArgument(
				    fmt::format("Graph: {} exists and will not override it!", dbName));
			}
		}
	        s = skg::SkgDB::Create(dbName,this->options); 
		if (!s.ok()) 
		{
		    std::cout << s.ToString() << std::endl;
		}
	        s = skg::SkgDB::Open(dbName,this->options,&(this->db));
		if (!s.ok()) 
		{
		    std::cout << s.ToString() << std::endl;
		}
	        s = db->CreateNewVertexLabel(this->v_label);
		if (!s.ok()) {
		    std::cout << s.ToString() << std::endl;
		}
	        s = db->CreateNewEdgeLabel(this->e_label, this->v_label, this->v_label);
		if (!s.ok()) {
		    std::cout << s.ToString() << std::endl;
		}
	    };

	    SkgGraph(std::string inDBName)
	    {
                this->options.LoadOptions();
                std::string dbName = inDBName;
                this->db_dir = options.GetDBDir(dbName);
	        s = skg::SkgDB::Open(dbName,this->options,&(this->db));
		if (!s.ok()) 
		{
		    std::cout << s.ToString() << std::endl;
		}
	    };

	    bool AddEdge(const char* srcStr, const char *tgtStr)
	    {
	        EdgeRequest req;
	        req.DisableWAL();
	        req.SetEdge(this->e_label, this->v_label, srcStr, this->v_label, tgtStr);
	        s = db->AddEdge(req);
		if (!s.ok()) 
		{
		    std::cout << s.ToString() << std::endl;
		    return false;
		}
		return true;
	    };

	    void PrInNbr(const char* center)
	    {
		TraverseRequest t_req;
		t_req.id = center;
		t_req.label = this->v_label;
		t_req.k = 1;
		t_req.qcols = std::vector<std::string>(0);
		t_req.direction = 'i';
		t_req.nlimit = this->options.nlimit;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		db->Kout(t_req, &vpair_vec);
		for (size_t i = 0; i < vpair_vec.size(); i ++) {
		    std::cout << i << ":" << vpair_vec[i].to_str() << std::endl;
		}
	    }

	    void PrOutNbr(const char* center)
	    {
		TraverseRequest t_req;
		t_req.id = center;
		t_req.label = this->v_label;
		t_req.k = 1;
		t_req.qcols = std::vector<std::string>(0);
		t_req.nlimit = this->options.nlimit;
		t_req.direction = 'o';
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		db->Kout(t_req, &vpair_vec);
		for (size_t i = 0; i < vpair_vec.size(); i ++) {
		    std::cout << i << ":" << vpair_vec[i].to_str() << std::endl;
		}
		std::cout << "#edges= " << vpair_vec.size() << std::endl;
	    }

	    ~SkgGraph()
	    {
	        db->Flush();
	        db->Close();
	    };


	    void AddEdges(IdArray src_ids, IdArray dst_ids) 
	    {
		CHECK(!read_only_) << "Graph is read-only. Mutations are not allowed.";
		CHECK(IsValidIdArray(src_ids)) << "Invalid src id array.";
		CHECK(IsValidIdArray(dst_ids)) << "Invalid dst id array.";
		  const auto srclen = src_ids->shape[0];
		  const auto dstlen = dst_ids->shape[0];
		  const int64_t* src_data = static_cast<int64_t*>(src_ids->data);
		  const int64_t* dst_data = static_cast<int64_t*>(dst_ids->data);
		  char uid[127],vid[127];
		  if (srclen == 1) {
		    // one-many
		    for (int64_t i = 0; i < dstlen; ++i) {
		      sprintf(uid, "%llu",src_data[0]);
		      sprintf(vid, "%llu",dst_data[i]);
		      AddEdge(uid,vid);
		    }
		  } else if (dstlen == 1) {
		    // many-one
		    for (int64_t i = 0; i < srclen; ++i) {
		      sprintf(uid, "%llu",src_data[i]);
		      sprintf(vid, "%llu",dst_data[0]);
		      AddEdge(uid,vid);
		    }
		  } else {
		    // many-many
		    CHECK(srclen == dstlen) << "Invalid src and dst id array.";
		    for (int64_t i = 0; i < srclen; ++i) {
		      sprintf(uid, "%llu",src_data[i]);
		      sprintf(vid, "%llu",dst_data[i]);
		      AddEdge(uid,vid);
		    }
		  }
	    };

	    bool HasVertex(const char* vidstr)
	    {
		vid_t max_vid=db->GetNumVertices()-1;
		bool ret;
		std::shared_ptr<IDEncoder> pIdEncoder=db->GetIDEncoder();
		Status s;
		vid_t vid;
		s=pIdEncoder->GetIDByVertex("v",vidstr,&vid);
		bool isIDLongStr= this->options.id_type==Options::VertexIdType::LONG;
		if (isIDLongStr)
			ret = vid<=max_vid?true:false;
		else
			ret=s.ok();
			return ret;
	    };

	    std::vector<bool> HasVertices(std::vector<std::string> vids)
	    {
		vid_t max_vid=db->GetNumVertices()-1;
		std::vector<bool> ret;
		std::shared_ptr<IDEncoder> pIdEncoder=db->GetIDEncoder();
		std::vector<std::string>::iterator vecIter;
		Status s;
		bool isIDLongStr= this->options.id_type==Options::VertexIdType::LONG;
		for(vecIter=vids.begin();vecIter!=vids.end();vecIter++)
		{
				vid_t vid;
				s=pIdEncoder->GetIDByVertex("v",*vecIter,&vid);
				if (isIDLongStr)
					ret.push_back( vid<=max_vid?true:false);
				else
					ret.push_back(s.ok());
		}
			return std::move(ret);
	    };

	    bool HasEdgeBetween(const char* srcStr, const char *tgtStr)
	    {
	        if (!HasVertex(srcStr) || !HasVertex(tgtStr)) return false;
		TraverseRequest t_req;
		t_req.id = srcStr;
		t_req.label = this->v_label;
		t_req.k = 1;
		t_req.qcols = std::vector<std::string>(0);
		t_req.direction = 'o';
		t_req.nlimit = this->options.nlimit;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		db->Kout(t_req, &vpair_vec);
		for (size_t i = 0; i < vpair_vec.size(); i ++) 
		    if ( vpair_vec[i].second.id.compare(tgtStr) ) return true;
		return false;
	    };

	    bool HasEdgeBetween(std::string srcStr,  std::string tgtStr)
	    {
		    return HasEdgeBetween(srcStr.c_str(),tgtStr.c_str());
	    }


	    std::vector<bool> HasEdgeBetween(std::vector<std::string> srcStrVec, std::vector<std::string> tgtStrVec)
	    {
		TraverseRequest t_req;
		t_req.label = this->v_label;
		t_req.k = 1;
		t_req.qcols = std::vector<std::string>(0);
		t_req.nlimit = this->options.nlimit;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		std::vector<bool> ret;
		if ( srcStrVec.size()==1 )
		{
	            ret.reserve(tgtStrVec.size());	
		    t_req.id = *srcStrVec.begin();
		    t_req.direction = 'o';
		    db->Kout(t_req, &vpair_vec);
		    for (size_t i = 0; i < tgtStrVec.size(); i ++) 
		    {
			    for (size_t j = 0; j < vpair_vec.size(); j ++) 
				if ( vpair_vec[j].second.id.compare(tgtStrVec[i])==0 )
				{
				    ret[i]=true;
				    break;
				}
			    ret[i]=false;
		    }
		}
		else
		{
		    if ( tgtStrVec.size()==1 )
		    {
	                ret.reserve(srcStrVec.size());	
		        t_req.id = *tgtStrVec.begin();
		        t_req.direction = 'i';
		        db->Kout(t_req, &vpair_vec);
		        for (size_t i = 0; i < srcStrVec.size(); i ++) 
		        {
			    for (size_t j = 0; j < vpair_vec.size(); j ++) 
				if ( vpair_vec[j].second.id.compare(srcStrVec[i])==0 )
				{
				    ret[i]=true;
				    break;
				}
			    ret[i]=false;
		        }
		    }
		    else
		    {
		        if ( tgtStrVec.size()==srcStrVec.size() )
			{
		            for (size_t i = 0; i < srcStrVec.size(); i ++) 
		            {
			        ret[i]=HasEdgeBetween(srcStrVec[i],tgtStrVec[i]);
			    }
			}//if
			SKG_LOG_ERROR("Wrong input into HasEdgeBetween: unequal vector size {}!={}",srcStrVec.size(),tgtStrVec.size());
		    }//if tgtStrVec == 1
		}//if srcStrVec == 1
	    };


	    std::vector<vid_t> Predecessors(const char* center, int nHops)
	    {
	        std::vector<vid_t> ret;
		TraverseRequest t_req;
		t_req.id = center;
		t_req.label = this->v_label;
		t_req.k = nHops;
		t_req.qcols = std::vector<std::string>(0);
		t_req.direction = 'i';
		t_req.nlimit = this->options.nlimit;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		db->Kout(t_req, &vpair_vec);
		vid_t vid;
		std::shared_ptr<IDEncoder> pIdEncoder=db->GetIDEncoder();
		Status s;
		bool isIDLongStr= this->options.id_type==Options::VertexIdType::LONG;
		for (size_t i = 0; i < vpair_vec.size(); i ++) {
		    s=pIdEncoder->GetIDByVertex("v", vpair_vec[i].first.id ,&vid);
		    if (isIDLongStr)
			ret.push_back( vid );
		    else
			if (!s.ok())
			    SKG_LOG_ERROR("No support to convert ID for {}", vpair_vec[i].first.id);
			else
			    ret.push_back(vid);
		}
		return std::move(ret);
	    };


	    std::vector<vid_t> Successors(const char* center, int nHops)
	    {
	        std::vector<vid_t> ret;
		TraverseRequest t_req;
		t_req.id = center;
		t_req.label = this->v_label;
		t_req.k = nHops;
		t_req.qcols = std::vector<std::string>(0);
		t_req.direction = 'o';
		t_req.nlimit = this->options.nlimit;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		db->Kout(t_req, &vpair_vec);
		vid_t vid;
		std::shared_ptr<IDEncoder> pIdEncoder=db->GetIDEncoder();
		Status s;
		bool isIDLongStr= this->options.id_type==Options::VertexIdType::LONG;
		for (size_t i = 0; i < vpair_vec.size(); i ++) {
		    s=pIdEncoder->GetIDByVertex("v", vpair_vec[i].second.id ,&vid);
		    if (isIDLongStr)
			ret.push_back( vid );
		    else
		    {
			if (!s.ok())
			    SKG_LOG_ERROR("No support yet to convert ID for {}", vpair_vec[i].second.id);
			else
			    ret.push_back(vid);
		    }
		}
		return std::move(ret);
	    };

	    void PrNbrInfo(const char* center, const char* vlabel, int hop)
	    {
		TraverseRequest t_req;
		t_req.id = center;
		t_req.label = vlabel;
		t_req.k = hop;
	        const std::string query_columns = "*";
		std::vector<std::string> qcols;
	        StringUtils::split(query_columns, ',', qcols);
		t_req.qcols = qcols;
		t_req.direction = 'o';
		t_req.nlimit = this->options.nlimit;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PathVertex> pv_vec;
		s = db->Kneighbor(t_req, &pv_vec);
		if (!s.ok()) {
		    std::cout << "status not ok" << std::endl;
		    std::cout << s.ToString() << std::endl;
		}
		for (size_t i = 0; i < pv_vec.size(); i ++) {
		    std::cout << i << ":" << pv_vec[i].to_str() << std::endl;
		}
		std::cout << "v_size = " << pv_vec.size() << std::endl;
	    }

	private:
	    skg::Options options;
            std::string db_dir;
	    const std::string v_label = "v";
	    const std::string e_label = "e";
	    skg::SkgDB *db = nullptr;
	    Status s;


	    //attributes origining from DGL graph
	    bool read_only_ = false;
};

#endif
