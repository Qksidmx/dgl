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

//
#include "fs/SubEdgePartition.h"
#include "fs/VertexColumnList.h"
#include "fs/ShardTree.h"

using namespace skg;

class SkgGraph{
	public:
	    SkgGraph()
	    {
                this->options.LoadOptions();
                this->options.force_create = true;
                std::string dbName = "t1";
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
		t_req.nlimit = 50000;
		t_req.label_constraint = std::vector<std::string>(0);
		std::vector<PVpair> vpair_vec;
		db->Kout(t_req, &vpair_vec);
		for (size_t i = 0; i < vpair_vec.size(); i ++) {
		    std::cout << i << ":" << vpair_vec[i].to_str() << std::endl;
		}
		std::cout << "#edges= " << vpair_vec.size() << std::endl;
	    }

	    void PrOutNbr(const char* center)
	    {
		TraverseRequest t_req;
		t_req.id = center;
		t_req.label = this->v_label;
		t_req.k = 1;
		t_req.qcols = std::vector<std::string>(0);
		t_req.direction = 'o';
		t_req.nlimit = 50000;
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
	private:
	    skg::Options options;
            std::string db_dir;
	    const std::string v_label = "v";
	    const std::string e_label = "e";
	    skg::SkgDB *db = nullptr;
	    Status s;
};

#endif
