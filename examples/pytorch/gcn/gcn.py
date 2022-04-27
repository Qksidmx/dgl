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
fn.sum()



def gcn_msg(edges):
    """
    这里表示，对于一个节点，将所有指向该节点的邻居节点的'h'信息(h即为节点的中间表达)，放到到'm'里面
    'm'全称是mailbox，意义是，对于一个节点，它的中间表达将由'm'区域内的所有节点表示
    例如：
    对于图 0->2, 1->2 
    那么，节点2的'm'区域就要包含节点0和节点1的'h'信息

    """
    return {'m' : edges.src['h']}
    
def gcn_reduce(nodes):
    """
    这里表示，对于一个节点，将其'm'区域的节点信息聚合，作为该节点的'h'信息(及该节点的中间表达)
    这个函数里的'm'、'h'和gcn_msg函数里的'm'、'h'
    本实验中，聚合方式为sum
    """
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

        
        """
         gcn的每一层就是基础的nn.Linear，详见 NodeApplyModule类
        """
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
            
            """
            
            利用update_all函数更新全图的节点特征，参考文档 ：https://docs.dgl.ai/en/0.8.x/generated/dgl.DGLGraph.update_all.html#
            函数原型： DGLGraph.update_all(message_func, reduce_func, apply_node_func=None, etype=None)
            函数处理步骤：
            ①计算所有邻居给自己带来的信息
            ②聚合这些信息

            该函数有前2个入参，是必要的，类型都是函数：

            第一个入参表示聚合该节点的信息来源，本实验里，该节点的信息来源为指向该节点的邻居，例如对于边信息
            [[0,1,2],
            [1,2,3]] 
            表示0->1, 1->2, 2->3这三条边，(DGL是单向图)
            那么对于节点0，无任何节点指向它，则信息源为空
            对于节点1，有节点0指向它，则其信息来源为0... 以此类推

            第二个入参表示信息聚合的方式，例如sum,mean等，表示如何聚合指向该节点的邻居节点信息

            第三个入参是可选的，表示更新完节点信息之后，再执行第三个函数来更新各个节点，本实验里即利用gcn更新

            """
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
    # load and preprocess dataset
    data = load_data(args)
    

    # 载入相关数据
    features = torch.FloatTensor(data.features)
    labels = torch.LongTensor(data.labels)
    train_mask = torch.ByteTensor(data.train_mask)
    val_mask = torch.ByteTensor(data.val_mask)
    test_mask = torch.ByteTensor(data.test_mask)
    in_feats = features.shape[1]
    n_classes = data.num_labels
    n_edges = data.graph.number_of_edges()

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
    g = DGLGraph(data.graph)
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
