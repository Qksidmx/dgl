"""
Semi-Supervised Classification with Graph Convolutional Networks
Paper: https://arxiv.org/abs/1609.02907
Code: https://github.com/tkipf/gcn

GCN with batch processing
"""
import argparse
import numpy as np
import time
import torch
import torch.nn as nn
import torch.nn.functional as F
from dgl import DGLGraph
from dgl.data import register_data_args, load_data
import dgl.function as fn
import pandas as pd
import dgl


def gcn_msg(edges):
    return {'m' : edges.src['h']}
    
def gcn_reduce(nodes):
    return {'h' : torch.sum(nodes.mailbox['m'], 1)}

class NodeApplyModule(nn.Module):
    def __init__(self, in_feats, out_feats, activation=None):
        super(NodeApplyModule, self).__init__()

        # 普通的nn.Linear层
        self.linear = nn.Linear(in_feats, out_feats)
        self.activation = activation

    def forward(self, nodes):
        # normalization by square root of dst degree

        # 在forward的时候，节点特征会先乘sqrt(D),是构建图之后，计算得到的
        h = nodes.data['h'] * nodes.data['norm']
        h = self.linear(h)
        if self.activation:
            h = self.activation(h)
        return {'h' : h}

class GCN(nn.Module):
    def __init__(self,
                 g,
                 in_feats,
                 n_hidden,
                 n_classes,
                 n_layers,
                 activation,
                 dropout):
        super(GCN, self).__init__()
        self.g = g

        # dropout系数
        if dropout:
            self.dropout = nn.Dropout(p=dropout)
        else:
            self.dropout = 0.
        self.layers = nn.ModuleList()

        # input layer
        self.layers.append(NodeApplyModule(in_feats, n_hidden, activation))

        # hidden layers
        for i in range(n_layers - 1):
            self.layers.append(NodeApplyModule(n_hidden, n_hidden, activation))

        # output layer
        self.layers.append(NodeApplyModule(n_hidden, n_classes))

    def forward(self, features):
        
        self.g.ndata['h'] = features

        for idx, layer in enumerate(self.layers):
            # apply dropout
            if idx > 0 and self.dropout:
                self.g.ndata['h'] = self.dropout(self.g.ndata['h'])
            # normalization by square root of src degree
            self.g.ndata['h'] = self.g.ndata['h'] * self.g.ndata['norm']
            self.g.update_all(gcn_msg, gcn_reduce, layer)
        return self.g.ndata.pop('h')

def evaluate(model, features, labels, mask):
    model.eval()
    with torch.no_grad():
        logits = model(features)
        logits = logits[mask]
        labels = labels[mask]
        _, indices = torch.max(logits, dim=1)
        correct = torch.sum(indices == labels)
        return correct.item() * 1.0 / len(labels)

def main(args):

    labels = pd.read_csv('/workspace/dataset/ogbn_mag/raw/node-label/paper/node-label.csv.gz', compression='gzip', header = None).values
    features = pd.read_csv('/workspace/dataset/ogbn_mag/raw/node-feat/paper/node-feat.csv.gz', compression='gzip', header = None).values
    
    
    train_idx = pd.read_csv('/workspace/dataset/ogbn_mag/split/time/paper/train.csv.gz', compression='gzip', header = None).values.T[0]
    valid_idx = pd.read_csv('/workspace/dataset/ogbn_mag/split/time/paper/train.csv.gz', compression='gzip', header = None).values.T[0]
    test_idx = pd.read_csv('/workspace/dataset/ogbn_mag/split/time/paper/train.csv.gz', compression='gzip', header = None).values.T[0]
    num_nodes,in_feats =features.shape
    n_classes = labels.max() + 1
    train_mask = torch.zeros((num_nodes,), dtype=torch.bool)
    val_mask = torch.zeros((num_nodes,), dtype=torch.bool)
    test_mask = torch.zeros((num_nodes,), dtype=torch.bool)
    features = torch.FloatTensor(features)
    labels = torch.LongTensor(labels)
    labels = labels.squeeze()

    train_mask[train_idx] = True
    val_mask[valid_idx] = True
    test_mask[test_idx] = True
    edges = pd.read_csv('/workspace/dataset/ogbn_mag/raw/relations/paper___cites___paper/edge.csv.gz', compression='gzip', header = None).values.T.astype(np.int64)

    # cuda相关
    if args.gpu < 0:
        cuda = False
    else:
        cuda = True
        torch.cuda.set_device(args.gpu)
        features = features.cuda()
        labels = labels.cuda()
        train_mask = train_mask.cuda()
        val_mask = val_mask.cuda()
        test_mask = test_mask.cuda()

    # graph preprocess and calculate normalization factor
    g = dgl.DGLGraph()
    g.add_nodes(num_nodes)
    g.add_edges(edges[0],edges[1])
    n_edges = g.number_of_edges()
    # add self loop
    g.add_edges(g.nodes(), g.nodes())
    # normalization ,得到开根号后的度矩阵 sqrt(D)
    degs = g.in_degrees().float()
    norm = torch.pow(degs, -0.5)
    norm[torch.isinf(norm)] = 0
    if cuda:
        norm = norm.cuda()
    g.ndata['norm'] = norm.unsqueeze(1)

    # create GCN model
    model = GCN(g,
                in_feats,
                args.n_hidden,
                n_classes,
                args.n_layers,
                F.relu,
                args.dropout)

    if cuda:
        model.cuda()

    # use optimizer
    optimizer = torch.optim.Adam(model.parameters(),
                                 lr=args.lr,
                                 weight_decay=args.weight_decay)

    # initialize graph
    dur = []
    for epoch in range(args.n_epochs):
        model.train()
        if epoch >= 3:
            t0 = time.time()
        # forward
        logits = model(features)
        logp = F.log_softmax(logits, 1)
        loss = F.nll_loss(logp[train_mask], labels[train_mask])

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if epoch >= 3:
            dur.append(time.time() - t0)

        acc = evaluate(model, features, labels, val_mask)
        print("Epoch {:05d} | Time(s) {:.4f} | Loss {:.4f} | Accuracy {:.4f} | "
              "ETputs(KTEPS) {:.2f}".format(epoch, np.mean(dur), loss.item(),
                                            acc, n_edges / np.mean(dur) / 1000))

    print()
    acc = evaluate(model, features, labels, test_mask)
    print("Test Accuracy {:.4f}".format(acc))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GCN')
    register_data_args(parser)
    parser.add_argument("--dropout", type=float, default=0.5,
            help="dropout probability")
    parser.add_argument("--gpu", type=int, default=-1,
            help="gpu")
    parser.add_argument("--lr", type=float, default=1e-2,
            help="learning rate")
    parser.add_argument("--n-epochs", type=int, default=200,
            help="number of training epochs")
    parser.add_argument("--n-hidden", type=int, default=16,
            help="number of hidden gcn units")
    parser.add_argument("--n-layers", type=int, default=1,
            help="number of hidden gcn layers")
    parser.add_argument("--weight-decay", type=float, default=5e-4,
            help="Weight for L2 loss")
    args = parser.parse_args()
    print(args)

    main(args)





"""
dataset/
`-- ogbn_mag
    |-- RELEASE_v2.txt
    |-- mapping
    |   |-- README.md
    |   |-- author_entidx2name.csv.gz
    |   |-- field_of_study_entidx2name.csv.gz
    |   |-- institution_entidx2name.csv.gz
    |   |-- labelidx2venuename.csv.gz
    |   |-- paper_entidx2name.csv.gz
    |   `-- relidx2relname.csv.gz
    |-- processed
    |   `-- dgl_data_processed
    |-- raw
    |   |-- node-feat
    |   |   `-- paper
    |   |       |-- node-feat.csv.gz
    |   |       `-- node_year.csv.gz
    |   |-- node-label
    |   |   `-- paper
    |   |       `-- node-label.csv.gz
    |   |-- nodetype-has-feat.csv.gz
    |   |-- nodetype-has-label.csv.gz
    |   |-- num-node-dict.csv.gz
    |   |-- relations
    |   |   |-- author___affiliated_with___institution
    |   |   |   |-- edge.csv.gz
    |   |   |   |-- edge_reltype.csv.gz
    |   |   |   `-- num-edge-list.csv.gz
    |   |   |-- author___writes___paper
    |   |   |   |-- edge.csv.gz
    |   |   |   |-- edge_reltype.csv.gz
    |   |   |   `-- num-edge-list.csv.gz
    |   |   |-- paper___cites___paper
    |   |   |   |-- edge.csv.gz
    |   |   |   |-- edge_reltype.csv.gz
    |   |   |   `-- num-edge-list.csv.gz
    |   |   `-- paper___has_topic___field_of_study
    |   |       |-- edge.csv.gz
    |   |       |-- edge_reltype.csv.gz
    |   |       `-- num-edge-list.csv.gz
    |   `-- triplet-type-list.csv.gz
    `-- split
        `-- time
            |-- nodetype-has-split.csv.gz
            `-- paper
                |-- test.csv.gz
                |-- train.csv.gz
                `-- valid.csv.gz
                """