"""GCN using DGL nn package

References:
- Semi-Supervised Classification with Graph Convolutional Networks
- Paper: https://arxiv.org/abs/1609.02907
- Code: https://github.com/tkipf/gcn
"""
import torch
import torch.nn as nn
from dgl.nn.pytorch import GraphConv


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
        self.layers = nn.ModuleList()
        # input layer
        self.layers.append(GraphConv(in_feats, n_hidden, activation=activation))
        # hidden layers
        for i in range(n_layers - 1):
            self.layers.append(GraphConv(n_hidden, n_hidden, activation=activation))
        # output layer
        self.layers.append(GraphConv(n_hidden, n_classes))
        self.dropout = nn.Dropout(p=dropout)

    # def forward(self, features):
    #     h = features
    #     for i, layer in enumerate(self.layers):
    #         if i != 0:
    #             h = self.dropout(h)
    #         h = layer(self.g, h)
    #     return h

    # def forward(self, features, ids=None):
    #     if ids is None:
    #         input_g = self.g
    #         h = features
    #     else:
    #         input_g = self.g.subgraph(ids)
    #         input_g = input_g.remove_self_loop()
    #         input_g = input_g.add_self_loop()
    #         h = features[ids]
    #
    #     for i, layer in enumerate(self.layers):
    #         if i != 0:
    #             h = self.dropout(h)
    #         h = layer(input_g, h)
    #     return h

    def forward(self, features, sub_graph=None):
        if sub_graph is None:
            g = self.g

        else:
            g = sub_graph

        h = features

        for i, layer in enumerate(self.layers):
            if i != 0:
                h = self.dropout(h)
            h = layer(g, h)
        return h
