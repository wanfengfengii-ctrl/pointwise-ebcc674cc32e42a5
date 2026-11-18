"""固定快照两仪器对照的配对规划。

表示不变量（生产表示如何维持）：

1. 地址即逻辑位置。collect_occurrences 在读取逻辑行之后按
   [batch_no, 零起行号, 零起列表项号] 生成地址；NULL列表与空列表没有
   出现项，被NULL列表遮蔽的物理槽不计入，NULL列表项仍是一个出现项。
   关系只引用地址，不按值标识出现项，重复读数各自保留、不会被去重。
2. 三级目标。_match_group 用最小费用最大流保证"先最大化配对数、再最小化
   总绝对差"；在并列最优解上按候选地址对（左地址、右地址）的字典序逐个
   强制，得到按左地址排序后字典序最小的地址对序列，与读数值次序无关。
3. 交付顺序与覆盖。plan_pairs 输出的 matches 按左地址升序，unmatched
   左侧先于右侧、同侧地址升序；每侧配对地址与未配对地址互斥且恰好覆盖
   该侧全部出现项。训练交付（Lake.export_pairs）把这里的地址关系与自足
   来源一起交付，独立解码（decode_export）从来源重新推导出现项并核对
   这些不变量，无需重新求最优。
"""
import heapq
from columnlake.source.events import DataError
from columnlake.source.schema import logical_type


class Occurrence:
    """一次观测出现项：逻辑地址加配对所需的逻辑值。"""

    __slots__ = ('address', 'sample', 'category', 'uv', 'item')

    def __init__(self, address, sample, category, uv, item):
        self.address = address
        self.sample = sample
        self.category = category
        self.uv = uv
        self.item = item


def require_pair_schema(schema):
    """检查选中批次的逻辑类型：sample:int 且
    observations:list<struct<category:text,uv:int>>。

    字段次序不限，允许额外字段以及任意位置的字典包装；按解释后的逻辑
    类型判断，不看当前数据值；零行批次同样检查。
    """
    fields = dict(schema.fields)
    sample = fields.get('sample')
    observations = fields.get('observations')
    if sample is None or observations is None:
        raise DataError('pair batch schema must offer sample and observations')
    if logical_type(sample).kind != 'int':
        raise DataError('pair batch sample must be int')
    observations = logical_type(observations)
    if observations.kind != 'list' or observations.item.kind != 'struct':
        raise DataError('pair batch observations must be a list of structs')
    item_fields = dict(observations.item.fields)
    category = item_fields.get('category')
    uv = item_fields.get('uv')
    if category is None or category.kind != 'text':
        raise DataError('pair observation category must be text')
    if uv is None or uv.kind != 'int':
        raise DataError('pair observation uv must be int')


def collect_occurrences(batch_no, rows):
    """从逻辑行提取出现项；NULL列表与空列表无出现项，NULL列表项仍是出现项。"""
    result = []
    for row_index, row in enumerate(rows):
        sample = row['sample']
        observations = row['observations']
        if observations is None:
            continue
        for item_index, item in enumerate(observations):
            if item is None:
                category = None
                uv = None
            else:
                category = item['category']
                uv = item['uv']
            result.append(Occurrence((batch_no, row_index, item_index),
                                     sample, category, uv, item))
    return result


def _min_cost_matching(m, n, cost, avail_left, avail_right):
    """二分图最小费用最大流。cost[i][j] 为 None 表示不可连边。

    返回 (最大配对数, 该配对数下的最小总费用)。费用非负，逐条最短路
    增广（Dijkstra + 势）即为该流量下的最小费用。
    """
    left_count = len(avail_left)
    right_count = len(avail_right)
    size = left_count + right_count + 2
    source = 0
    sink = size - 1
    graph = [[] for _ in range(size)]

    def link(u, v, capacity, weight):
        graph[u].append([v, capacity, weight, len(graph[v])])
        graph[v].append([u, 0, -weight, len(graph[u]) - 1])

    for a in range(left_count):
        link(source, 1 + a, 1, 0)
    for b in range(right_count):
        link(1 + left_count + b, sink, 1, 0)
    for a, i in enumerate(avail_left):
        row = cost[i]
        for b, j in enumerate(avail_right):
            weight = row[j]
            if weight is not None:
                link(1 + a, 1 + left_count + b, 1, weight)
    potential = [0] * size
    flow = 0
    total = 0
    while True:
        dist = [None] * size
        dist[source] = 0
        previous = [None] * size
        queue = [(0, source)]
        while queue:
            d, u = heapq.heappop(queue)
            if d != dist[u]:
                continue
            for index, edge in enumerate(graph[u]):
                if edge[1] <= 0:
                    continue
                v = edge[0]
                candidate = d + edge[2] + potential[u] - potential[v]
                if dist[v] is None or candidate < dist[v]:
                    dist[v] = candidate
                    previous[v] = (u, index)
                    heapq.heappush(queue, (candidate, v))
        if dist[sink] is None:
            return flow, total
        for v in range(size):
            if dist[v] is not None:
                potential[v] += dist[v]
        v = sink
        while v != source:
            u, index = previous[v]
            edge = graph[u][index]
            edge[1] -= 1
            graph[v][edge[3]][1] += 1
            total += edge[2]
            v = u
        flow += 1


def _match_group(left_items, right_items, tolerance):
    """同组出现项的三级目标配对。

    返回 (pairs, rest_left, rest_right)：pairs 为按左地址排序的
    (左出现项, 右出现项) 列表；其余出现项保持地址升序。
    """
    left_items = sorted(left_items, key=lambda occ: occ.address)
    right_items = sorted(right_items, key=lambda occ: occ.address)
    m = len(left_items)
    n = len(right_items)
    cost = []
    for left in left_items:
        row = []
        for right in right_items:
            distance = abs(left.uv - right.uv)
            row.append(distance if distance <= tolerance else None)
        cost.append(row)
    all_left = list(range(m))
    all_right = list(range(n))
    remaining = _min_cost_matching(m, n, cost, all_left, all_right)
    used_left = [False] * m
    used_right = [False] * n
    forced = []
    # 按 (左地址, 右地址) 字典序考察候选对：若某个并列最优解包含它，
    # 则强制纳入。归纳可证最终强制集即字典序最小的最优地址对序列。
    for i in range(m):
        if remaining[0] == 0:
            break
        for j in range(n):
            if used_right[j] or cost[i][j] is None:
                continue
            trial_left = [x for x in all_left if x != i and not used_left[x]]
            trial_right = [y for y in all_right if y != j and not used_right[y]]
            count, total = _min_cost_matching(m, n, cost, trial_left, trial_right)
            if 1 + count == remaining[0] and cost[i][j] + total == remaining[1]:
                forced.append((i, j))
                used_left[i] = True
                used_right[j] = True
                remaining = (count, total)
                break
    pairs = [(left_items[i], right_items[j]) for i, j in forced]
    rest_left = [left_items[i] for i in range(m) if not used_left[i]]
    rest_right = [right_items[j] for j in range(n) if not used_right[j]]
    return pairs, rest_left, rest_right


def _eligible(occ):
    return occ.sample is not None and occ.category is not None and occ.uv is not None


def plan_pairs(left_occurrences, right_occurrences, tolerance):
    """两仪器出现项的对照配对规划。

    返回 (matches, unmatched)：matches 为 [[左地址, 右地址], ...] 按左地址
    升序；unmatched 为 [[side, 地址], ...]，左侧先于右侧、同侧地址升序。
    每侧配对地址与未配对地址互斥且恰好覆盖该侧全部出现项。
    """
    left_groups = {}
    right_groups = {}
    unmatched_left = []
    unmatched_right = []
    for occ in left_occurrences:
        if _eligible(occ):
            left_groups.setdefault((occ.sample, occ.category), []).append(occ)
        else:
            unmatched_left.append(occ)
    for occ in right_occurrences:
        if _eligible(occ):
            right_groups.setdefault((occ.sample, occ.category), []).append(occ)
        else:
            unmatched_right.append(occ)
    pairs = []
    for key, group_left in left_groups.items():
        group_right = right_groups.pop(key, [])
        if not group_right:
            unmatched_left.extend(group_left)
            continue
        found, rest_left, rest_right = _match_group(group_left, group_right, tolerance)
        pairs.extend(found)
        unmatched_left.extend(rest_left)
        unmatched_right.extend(rest_right)
    for group_right in right_groups.values():
        unmatched_right.extend(group_right)
    pairs.sort(key=lambda pair: pair[0].address)
    unmatched_left.sort(key=lambda occ: occ.address)
    unmatched_right.sort(key=lambda occ: occ.address)
    matches = [[list(left.address), list(right.address)] for left, right in pairs]
    unmatched = [['left', list(occ.address)] for occ in unmatched_left]
    unmatched += [['right', list(occ.address)] for occ in unmatched_right]
    return matches, unmatched
