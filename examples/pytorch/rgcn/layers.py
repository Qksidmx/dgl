import torch
import torch.nn as nn
import dgl.function as fn

class RGCNLayer(nn.Module):
    def __init__(self, in_feat, out_feat, bias=None, activation=None,
                 self_loop=False, dropout=0.0):
        super(RGCNLayer, self).__init__()
        self.bias = bias
        self.activation = activation
        self.self_loop = self_loop

        # bias
        if self.bias == True:
            self.bias = nn.Parameter(torch.Tensor(out_feat))
            nn.init.xavier_uniform_(self.bias,
                                    gain=nn.init.calculate_gain('relu'))

        # weight for self loop
        # 如果有self loop，则创建self_W矩阵
        if self.self_loop:
            self.loop_weight = nn.Parameter(torch.Tensor(in_feat, out_feat))
            nn.init.xavier_uniform_(self.loop_weight,
                                    gain=nn.init.calculate_gain('relu'))

        if dropout:
            self.dropout = nn.Dropout(dropout)
        else:
            self.dropout = None

    # define how propagation is done in subclass
    def propagate(self, g):
        raise NotImplementedError

    def forward(self, g):

        """
        这个为rgcn真正forward的地方
        """

        # ① 计算self_message， 即是节点自身乘以self_W
        if self.self_loop:
            loop_message = torch.mm(g.ndata['h'], self.loop_weight)
            if self.dropout is not None:
                loop_message = self.dropout(loop_message)
                
        # ②核心的前向传播处理，在子类实现
        self.propagate(g)

        # apply bias and activation
        node_repr = g.ndata['h']
        if self.bias:
            node_repr = node_repr + self.bias
        if self.self_loop:
            node_repr = node_repr + loop_message
        if self.activation:
            node_repr = self.activation(node_repr)

        g.ndata['h'] = node_repr

class RGCNBasisLayer(RGCNLayer):
    def __init__(self, in_feat, out_feat, num_rels, num_bases=-1, bias=None,
                 activation=None, is_input_layer=False):
        """
        ①父类初始化，就是最基本的初始化W和b
        模型的forward是继承自父类
        """
        super(RGCNBasisLayer, self).__init__(in_feat, out_feat, bias, activation)
        self.in_feat = in_feat
        self.out_feat = out_feat
        self.num_rels = num_rels
        self.num_bases = num_bases
        self.is_input_layer = is_input_layer



        if self.num_bases <= 0 or self.num_bases > self.num_rels:
            self.num_bases = self.num_rels

        # add basis weights
        """
        创建参数矩阵，是三维的，num_bases是超参，感觉有点类似attention里的head数
        """
        self.weight = nn.Parameter(torch.Tensor(self.num_bases, self.in_feat,
                                                self.out_feat))
        
        """
        上一步构建了一个基础参数矩阵维度[num_bases, in_feats, out_feats]
        如果图的relation数大于给定的num_bases的话，则再创建一个分解矩阵，W_comp
        W_comp维度为[num_rel, num_bases]
        在后续的propagation的时候，利用W_comp，将weight的维度扩充[num_relation, in_feats, out_feats]

        这样做的好处是，在relation过于大的时候，能够有效减少参数量，例如：
        在一个场景中，relations=100， in_feats=20, out_feats= 20
        方案A：
            直接创建参数矩阵W = [100,20,20]
            参数为= 100*20*20 = 4万
        方案B：
            ①根据num_bases创建参数矩阵W = [50,20,20], (假设num_bases=50)
            ②创建W_comp矩阵, [100,50]
            
            ③在后续的propagation的时候，利用W_comp将W扩充至[100,20,20]
            参数总量为: 50*20*20 + 100* 50 = 2.5万

        所以方案B的参数量更少，相当于共享了一部分参数，该方案是论文里的basis decomposition
        """
        if self.num_bases < self.num_rels:
            # linear combination coefficients
            self.w_comp = nn.Parameter(torch.Tensor(self.num_rels,
                                                    self.num_bases))
        nn.init.xavier_uniform_(self.weight, gain=nn.init.calculate_gain('relu'))
        if self.num_bases < self.num_rels:
            nn.init.xavier_uniform_(self.w_comp,
                                    gain=nn.init.calculate_gain('relu'))

    def propagate(self, g):

        """
        这个if判断和上面方案B的处理相对应
        """
        if self.num_bases < self.num_rels:
            # generate all weights from bases
            # 将三维矩阵reshape至两维，方便下一步的W_comp和weight相乘
            weight = self.weight.view(self.num_bases,
                                      self.in_feat * self.out_feat)

            # 乘完再reshape回三维，对应上文方案B的步骤③
            weight = torch.matmul(self.w_comp, weight).view(
                                    self.num_rels, self.in_feat, self.out_feat)
        else:
            weight = self.weight

        """
        msg_func
        """
        if self.is_input_layer:
            def msg_func(edges):
                """
                输入层的msg_func，作用是将节点的node id转化成embed
                ①将 weight矩阵 [num_rel,in,out] reshape 成[num_rel * in, out]的形状
                    就是本身对于每一个relation，都有[in,out]的一个权重矩阵
                    这里reshape一下变成二维，
                    embed[0:in-1]代表relation 0 的权重矩阵
                    embed[in:2*in-1]代表relation 1 的权重矩阵
                    ...
                    embed[k*in:(k+1) * in-1]代表relation k 的权重矩阵

                ②计算index，
                    edges.data['type'] * self.in_feat 这个，套用上方的公式，edges.data['type'] 其实就是 k
                    因此，这里就是定位到第k个relations
                    进一步，edges.src['id']这个变量就是基于第k个relations的一个偏置量
                    
                    TODO：
                    源码注释里说是source node id，但是embed矩阵的行数是远远小于node id数的，所以这里没有很清楚，后续再看看
                
                ③根据index，取到embed，并乘一个norm系数返回。
                """
                # for input layer, matrix multiply can be converted to be
                # an embedding lookup using source node id

                # 
                embed = weight.view(-1, self.out_feat)


                index = edges.data['type'] * self.in_feat + edges.src['id']

                return {'msg': embed[index] * edges.data['norm']}
        else:
            def msg_func(edges):

                """
                中间层的msg_func
                ①根据edge_type从权重矩阵中取出一行
                ②节点的中间表达和权重矩阵相乘
                ③乘一个norm系数

                """
                # weight shape  = [num_rel,in,out] ，这里取edgetype对应的权重w
                # w shape = [1,in,out]
                w = weight[edges.data['type']]

                # torch的batch矩阵乘法 [B,n,m] × [B,m,p]  = [B,n,p]
                msg = torch.bmm(edges.src['h'].unsqueeze(1), w).squeeze()
                msg = msg * edges.data['norm']
                return {'msg': msg}
        
        # 更新全局节点
        # msg_func就是上文定义的， fn.sum代表将相关msg相加
        g.update_all(msg_func, fn.sum(msg='msg', out='h'), None)

class RGCNBlockLayer(RGCNLayer):
    def __init__(self, in_feat, out_feat, num_rels, num_bases, bias=None,
                 activation=None, self_loop=False, dropout=0.0):
        super(RGCNBlockLayer, self).__init__(in_feat, out_feat, bias,
                                             activation, self_loop=self_loop,
                                             dropout=dropout)
        self.num_rels = num_rels
        self.num_bases = num_bases
        assert self.num_bases > 0

        self.out_feat = out_feat
        self.submat_in = in_feat // self.num_bases
        self.submat_out = out_feat // self.num_bases

        # assuming in_feat and out_feat are both divisible by num_bases
        self.weight = nn.Parameter(torch.Tensor(
            self.num_rels, self.num_bases * self.submat_in * self.submat_out))
        nn.init.xavier_uniform_(self.weight, gain=nn.init.calculate_gain('relu'))

    def msg_func(self, edges):
        weight = self.weight[edges.data['type']].view(
                    -1, self.submat_in, self.submat_out)
        node = edges.src['h'].view(-1, 1, self.submat_in)
        msg = torch.bmm(node, weight).view(-1, self.out_feat)
        return {'msg': msg}

    def propagate(self, g):
        g.update_all(self.msg_func, fn.sum(msg='msg', out='h'), self.apply_func)

    def apply_func(self, nodes):
        return {'h': nodes.data['h'] * nodes.data['norm']}

