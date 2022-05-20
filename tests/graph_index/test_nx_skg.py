from __future__ import absolute_import

import sys
sys.path.insert(0,'../../python/build/lib')
import os
import numpy as np
import networkx as nx
import scipy.sparse as sp
from dgl.graph_index import create_graph_index
from dgl.skg_graph import create_skg_graph


def _encode_onehot(labels):
    classes = set(labels)
    classes_dict = {c: np.identity(len(classes))[i, :] for i, c in
                    enumerate(classes)}
    labels_onehot = np.array(list(map(classes_dict.get, labels)),
                             dtype=np.int32)
    return labels_onehot

default_dir = os.path.join(os.path.expanduser('~'), '.dgl')
coradir = os.environ.get('DGL_DOWNLOAD_DIR', default_dir)
if not os.path.exists(coradir):
    os.makedirs(coradir)
idx_features_labels = np.genfromtxt("{}/cora/cora.content".format(coradir), dtype=np.dtype(str))
features = sp.csr_matrix(idx_features_labels[:, 1:-1], dtype=np.float32) 
labels = _encode_onehot(idx_features_labels[:, -1])
num_labels = labels.shape[1]
idx = np.array(idx_features_labels[:, 0], dtype=np.int32)
idx_map = {j: i for i, j in enumerate(idx)}
edges_unordered = np.genfromtxt("{}/cora/cora.cites".format(coradir), dtype=np.int32)
edges = np.array(list(map(idx_map.get, edges_unordered.flatten())), dtype=np.int32).reshape(edges_unordered.shape)
adj = sp.coo_matrix((np.ones(edges.shape[0]), (edges[:, 0], edges[:, 1])), shape=(labels.shape[0], labels.shape[0]), dtype=np.float32)
adj = adj + adj.T.multiply(adj.T > adj) - adj.multiply(adj.T > adj)
nxg = nx.from_scipy_sparse_matrix(adj, create_using=nx.DiGraph())
skg = create_skg_graph()
skg.from_networkx(nxg)

