import argparse
import time
import numpy as np
import torch
import torch.nn.functional as F
import dgl
from dgl.data import CoraGraphDataset, CiteseerGraphDataset, PubmedGraphDataset

from gcn import GCN


# from gcn_mp import GCN
# from gcn_spmv import GCN


def evaluate(model, features, labels, mask, sub_graph=None):
    model.eval()
    with torch.no_grad():
        if sub_graph is None:
            logits = model(features)
        else:
            logits = model(features, sub_graph)
        logits = logits[mask]
        labels = labels[mask]
        _, indices = torch.max(logits, dim=1)
        correct = torch.sum(indices == labels)
        return correct.item() * 1.0 / len(labels)


def evaluate_v2(logits, labels):
    _, indices = torch.max(logits, dim=1)
    correct = torch.sum(indices == labels)
    return correct.item() * 1.0 / len(labels)


def sub_nodes(num_nodes, num_subgraphs):
    new_ids = torch.randperm(num_nodes)
    chunks = torch.chunk(new_ids, num_subgraphs)

    return chunks


def find_all_related_nodes(g, sub_nodes):
    '''
    根据给定的节点，找出所有指向该节点的邻居节点，并返回
    例如对于图([[0,1,2,3,4],[1,2,3,4,0]])
    输入节点0的话，返回所有0,4
    '''
    related_nodes = torch.tensor([], dtype=torch.int64)
    # for node in sub_nodes:
    #     src_nodes = g.in_edges(node)[0]
    #     related_nodes = torch.cat([related_nodes, src_nodes])

    related_nodes = g.in_edges(sub_nodes)[0]
    # 对related_nodes去重，并去除sub_nodes已包含的节点
    additional_nodes = set(related_nodes.numpy()) - set(sub_nodes.numpy())

    related_nodes = torch.tensor(list(additional_nodes))
    res = torch.cat([sub_nodes, related_nodes])
    return res


def sub_graph(g, node_ids):
    sg = g.subgraph(node_ids)

    sg = sg.remove_self_loop()
    sg = sg.add_self_loop()

    degs = sg.in_degrees().float()
    norm = torch.pow(degs, -0.5)
    norm[torch.isinf(norm)] = 0

    if args.gpu < 0:
        cuda = False
    else:
        cuda = True
    if cuda:
        norm = norm.cuda()

    sg.ndata['norm'] = norm.unsqueeze(1)

    return sg


def main(args):
    # load and preprocess dataset
    if args.dataset == 'cora':
        data = CoraGraphDataset()
    elif args.dataset == 'citeseer':
        data = CiteseerGraphDataset()
    elif args.dataset == 'pubmed':
        data = PubmedGraphDataset()
    else:
        raise ValueError('Unknown dataset: {}'.format(args.dataset))

    g = data[0]

    if args.gpu < 0:
        cuda = False
    else:
        cuda = True
        g = g.int().to(args.gpu)

    features = g.ndata['feat']
    labels = g.ndata['label']
    train_mask = g.ndata['train_mask']
    val_mask = g.ndata['val_mask']
    test_mask = g.ndata['test_mask']
    in_feats = features.shape[1]
    n_classes = data.num_labels
    n_edges = data.graph.number_of_edges()
    print("""----Data statistics------'
      #Edges %d
      #Classes %d
      #Train samples %d
      #Val samples %d
      #Test samples %d""" %
          (n_edges, n_classes,
           train_mask.int().sum().item(),
           val_mask.int().sum().item(),
           test_mask.int().sum().item()))

    # add self loop
    if args.self_loop:
        g = dgl.remove_self_loop(g)
        g = dgl.add_self_loop(g)
    n_edges = g.number_of_edges()

    # normalization
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
    loss_fcn = torch.nn.CrossEntropyLoss()

    # use optimizer
    optimizer = torch.optim.Adam(model.parameters(),
                                 lr=args.lr,
                                 weight_decay=args.weight_decay)

    # initialize graph
    dur = []

    if args.num_chunks:
        chunks = sub_nodes(g.num_nodes(), args.num_chunks)
        if args.related_nodes:
            related_nodes = [find_all_related_nodes(g, c) for c in chunks]
            sgs = [sub_graph(g, r) for r in related_nodes]
        else:
            related_nodes = chunks
            sgs = [sub_graph(g, c) for c in chunks]


    else:
        chunks = None

    for epoch in range(args.n_epochs):
        model.train()
        if epoch >= 3:
            t0 = time.time()

        if chunks is None:
            logits = model(g.ndata['feat'])
            loss = loss_fcn(logits[train_mask], labels[train_mask])
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        else:
            # loss = 0

            for c, r, sg in zip(chunks, related_nodes, sgs):
                logits = model(features[r], sg)
                chunk_labels = labels[c]

                loss = loss_fcn(logits[:len(c)][train_mask[c]], chunk_labels[train_mask[c]])
                # loss /= args.num_chunks
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
        if epoch >= 3:
            dur.append(time.time() - t0)
        if chunks is None:
            acc = evaluate(model, g.ndata['feat'], labels, val_mask)

        else:
            acc = 0
            model.eval()
            for c, r, sg in zip(chunks, related_nodes, sgs):
                logits = model(features[r], sg)
                chunk_labels = labels[c]
                acc += evaluate_v2(logits[:len(c)][val_mask[c]], chunk_labels[val_mask[c]])
            acc /= args.num_chunks

        print("Epoch {:05d} | Time(s) {:.4f} | Loss {:.4f} | Accuracy {:.4f} | "
              "ETputs(KTEPS) {:.2f}".format(epoch, np.mean(dur), loss.item(),
                                            acc, n_edges / np.mean(dur) / 1000))

    print()
    if chunks is None:
        acc = evaluate(model, g.ndata['feat'], labels, test_mask)
    else:
        acc = 0
        model.eval()
        for c, r, sg in zip(chunks, related_nodes, sgs):
            logits = model(features[r], sg)
            chunk_labels = labels[c]
            acc += evaluate_v2(logits[:len(c)][test_mask[c]], chunk_labels[test_mask[c]])
        acc /= args.num_chunks

    print("Test accuracy {:.2%}".format(acc))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GCN')
    parser.add_argument("--dataset", type=str, default="cora",
                        help="Dataset name ('cora', 'citeseer', 'pubmed').")
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
    parser.add_argument("--self-loop", action='store_true',
                        help="graph self-loop (default=False)")
    parser.add_argument("--num-chunks", type=int, default=0,
                        help="number of chunks")
    parser.add_argument("--related-nodes", type=bool, default=True,
                        help="whether to find all neighbors for each node")

    parser.set_defaults(self_loop=False)
    args = parser.parse_args()
    print(args)

    main(args)
