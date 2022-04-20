
/**
 * @file
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 *
 * @section DESCRIPTION
 *
 * Tools for listing the TOP K values from a verte data file.
 */

#ifndef DEF_GRAPHCHI_BPTOPLIST
#define DEF_GRAPHCHI_BPTOPLIST

#include <vector>
#include <algorithm>
#include <errno.h>
#include <assert.h>
#include <libgen.h>

#include "fs/stripedio.hpp"
#include "util/logger.h"
#include "util/merge.hpp"
#include "util/ioutil.h"
#include "util/qsort.hpp"
#include "gp/chifilenames.hpp"
#include "fs/vertex_data.h"

#include "util/pathutils.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "fs/Metadata.hpp"

namespace skg {

    template <typename VertexDataType>
    struct vertex_value {
        vid_t vertex;
        VertexDataType value;
        vertex_value() {}
        vertex_value(vid_t v, VertexDataType x) : vertex(v), value(x) {}
    };

    template <typename VertexDataType>
    bool vertex_value_greater(const vertex_value<VertexDataType> &a, const vertex_value<VertexDataType> &b) {
        return a.value > b.value;
    }

    /**
      * Reads the vertex data file and returns top N values.
      * Vertex value type must be given as a template parameter.
      * This method has been implemented in a manner to consume very little
      * memory, i.e the whole file is not loaded into memory (unless ntop = nvertices).
      * @param basefilename name of the graph
      * @param ntop number of top values to return (if ntop is smaller than the total number of vertices, returns all in sorted order)
      * @param from first vertex to include (default, 0)
      * @param to last vertex to include (default, all)
      * @return a vector of top ntop values
     */
    template <typename VertexDataType>
    std::vector<vertex_value<VertexDataType> > get_top_vertices(std::string basefilename, int ntop, vid_t from=0, vid_t to=0) {
        typedef vertex_value<VertexDataType> vv_t;

        /* Initialize striped IO manager */
        metrics m("toplist");
        stripedio * iomgr = new stripedio(m);

        /* Initialize the vertex-data reader */
        vid_t readwindow = 1024 * 1024;
        ///size_t numvertices = get_num_vertices(basefilename);
        MetaNumVertices vertices_info;
        MetadataFileHandler::ReadNumVertices(basefilename, &vertices_info);

        vertex_data_store<VertexDataType> * vertexdata =
        new vertex_data_store<VertexDataType>(basefilename, 
                vertices_info.max_allocated_vid, iomgr);

        if ((size_t)ntop > vertices_info.max_allocated_vid) {
            ntop = (int)vertices_info.max_allocated_vid;
        }

        /* Initialize buffer */
        vv_t * buffer_idxs = (vv_t*) calloc(readwindow, sizeof(vv_t));
        vv_t * topbuf = (vv_t*) calloc(ntop, sizeof(vv_t));
        vv_t * mergearr = (vv_t*) calloc(ntop * 2, sizeof(vv_t));

        /* Iterate the vertex values and maintain the top-list */
        size_t idx = 0;
        vid_t st = 0;
        vid_t en = vertices_info.max_allocated_vid - 1;

        int count = 0;
        while(st <= vertices_info.max_allocated_vid - 1) {
            en = st + readwindow - 1;
            if (en >= vertices_info.max_allocated_vid - 1) en = vertices_info.max_allocated_vid - 1;

            /* Load the vertex values */
            vertexdata->load(st, en);

            int nt = en - st + 1;
            int k = 0;
            VertexDataType minima = VertexDataType();
            if (count > 0) {
                minima = topbuf[ntop - 1].value; // Minimum value that should be even considered
            }
            for(int j=0; j < nt; j++) {
                VertexDataType& val = *vertexdata->vertex_data_ptr(j + st);
                if (count == 0 || (val > minima)) {
                    buffer_idxs[k] = vv_t((vid_t)idx + from, val);
                    k++;
                }
                idx++;
            }
            nt = k; /* How many were actually included */

            /* Sort buffer-idxs */
            quickSort(buffer_idxs, nt, vertex_value_greater<VertexDataType>);

            /* Merge the top with the current top */
            if (count == 0) {
                /* Nothing to merge, just copy */
                memcpy(topbuf, buffer_idxs, ntop * sizeof(vv_t));
            } else {
                // void merge(ET* S1, int l1, ET* S2, int l2, ET* R, F f) {
                merge<vv_t>(topbuf, ntop, buffer_idxs, std::min(ntop, nt), mergearr,  vertex_value_greater<VertexDataType>);
                memcpy(topbuf, mergearr, ntop * sizeof(vv_t));
            }

            count++;
            st += readwindow;
        }

        /* Return */
        std::vector< vv_t > ret;
        for(int i=0; i < ntop; i++) {
            ret.push_back(topbuf[i]);
        }
        free(buffer_idxs);
        free(mergearr);
        free(topbuf);

        delete vertexdata;
        delete iomgr;

        return ret;
    }

    template<typename VertexDataType>
    void dump_vids_data(
            std::set<vid_t> &vids, 
            std::string basefilepath, 
            int iteration, 
            vid_t from=0, 
            vid_t to=0) {

        SKG_LOG_INFO("======== get vertices data from vids ========", "");

        /* Initialize striped IO manager */
        metrics m("toplist");
        stripedio * iomgr = new stripedio(m);

        /* Initialize the vertex-data reader */
        vid_t readwindow = 1024 * 1024;
        ///size_t numvertices = get_num_vertices(basefilename);
        MetaNumVertices vertices_info;
        MetadataFileHandler::ReadNumVertices(basefilepath, &vertices_info);
        vertex_data_store<VertexDataType> * vertexdata =
            new vertex_data_store<VertexDataType>(basefilepath, vertices_info.max_allocated_vid, iomgr);
#if 1
        const std::string basefilename = PathUtils::os_path_basename(basefilepath);
        const std::string basedirname = PathUtils::os_path_dirname(basefilepath);
        const std::string voutdirname = basedirname + "/vout";
        const std::string outfilename = fmt::format("{0}/vout/vids-{0}.iter_{1}.vdata", basefilename, iteration);
        SKG_LOG_INFO("Writing to {}", outfilename);
        /* create folder if not exists */
        struct stat folder_st = {0};
        if (stat(voutdirname.c_str(), &folder_st) == -1) {
            SKG_LOG_INFO("create folder {}", voutdirname);
            mkdir(voutdirname.c_str(), 0777);
        } else {
            SKG_LOG_INFO("folder {} exists!", voutdirname);
        }
        FILE *outfile = fopen(outfilename.c_str(), "w");
        if (outfile == NULL) {
            SKG_LOG_ERROR("Open {} failed!", outfilename);
            return ;
        }
#endif

        /* Iterate the vertex values */
        vid_t st = 0;
        vid_t en = vertices_info.max_allocated_vid - 1;

        vid_t min_id = *(vids.begin());
        vid_t max_id = *(vids.end());
        while(st <= vertices_info.max_allocated_vid - 1) {
            en = st + readwindow - 1;
            if (en >= vertices_info.max_allocated_vid - 1) en = vertices_info.max_allocated_vid - 1;
            if (min_id > en) {
                continue;
            }
            if (max_id < st) {
                break;
            }
            /* Load the vertex values */
            vertexdata->load(st, en);

            int nt = en - st + 1;
            for(int j=0; j < nt; j++) {
                vid_t id = j + st;
                if (vids.find(id) == vids.end()) {
                   continue; 
                }
                const VertexDataType& val = *vertexdata->vertex_data_ptr(j + st);
                fprintf(outfile, "%d\t%s\n", j+st, val.to_str().c_str());
            }
            
            st += readwindow;
        }

        delete vertexdata;
        delete iomgr;
        fclose(outfile);
        SKG_LOG_INFO("======== get vids data end ========", "");
    }

    template <typename VertexDataType>
    void dump_vertices_data(std::string basefilepath, int iteration, vid_t from=0, vid_t to=0) {
        SKG_LOG_INFO("======== vertices data ========", "");

        /* Initialize striped IO manager */
        metrics m("toplist");
        stripedio * iomgr = new stripedio(m);

        /* Initialize the vertex-data reader */
        vid_t readwindow = 1024 * 1024;
        ///size_t numvertices = get_num_vertices(basefilename);
        MetaNumVertices vertices_info;
        MetadataFileHandler::ReadNumVertices(basefilepath, &vertices_info);
        vertex_data_store<VertexDataType> * vertexdata =
            new vertex_data_store<VertexDataType>(basefilepath, vertices_info.max_allocated_vid, iomgr);

        const std::string basefilename = PathUtils::os_path_basename(basefilepath);
        const std::string basedirname = PathUtils::os_path_dirname(basefilepath);
        const std::string voutdirname = basedirname + "/vout";
        const std::string outfilename = fmt::format("{0}/vout/{0}.iter_{1}.vdata", basedirname, iteration);
        SKG_LOG_INFO("Writing to {}", outfilename);
        /* create folder if not exists */
        struct stat folder_st = {0};
        if (stat(voutdirname.c_str(), &folder_st) == -1) {
            SKG_LOG_INFO("create folder {}", voutdirname);
            mkdir(voutdirname.c_str(), 0777);
        } else {
            SKG_LOG_INFO("folder {} exists!", voutdirname);
        }
        FILE *outfile = fopen(outfilename.c_str(), "w");
        if (outfile == NULL) {
            SKG_LOG_ERROR("Open {} failed!", outfilename);
            return ;
        }

        /* Iterate the vertex values */
        size_t idx = 0;
        vid_t st = 0;
        vid_t en = vertices_info.max_allocated_vid - 1;

        while(st <= vertices_info.max_allocated_vid - 1) {
            en = st + readwindow - 1;
            if (en >= vertices_info.max_allocated_vid - 1) en = vertices_info.max_allocated_vid - 1;

            /* Load the vertex values */
            vertexdata->load(st, en);

            int nt = en - st + 1;
            int k = 0;
            for(int j=0; j < nt; j++) {
                const VertexDataType& val = *vertexdata->vertex_data_ptr(j + st);
                // printf("v with data is [%d]:[%f]\n", j+st, val);
                fprintf(outfile, "%d\t%s\n", j+st, val.to_str().c_str());
//                fprintf(outfile, "%d\t%.2f\n", j + st, val);
                idx++;
            }
            nt = k; /* How many were actually included */

            st += readwindow;
        }

        delete vertexdata;
        delete iomgr;

        fclose(outfile);
        SKG_LOG_INFO("======== vertices data end ========", "");
    }


};

#endif

