"""独立小组穷举核账与表示不变量说明。

表示不变量（生产表示如何维持）：

1. 地址即逻辑位置。columnlake/delivery/pairing.py 的 collect_occurrences 在
   读取逻辑行之后按 [batch_no, 零起行号, 零起列表项号] 生成地址；NULL列表
   与空列表没有出现项，被NULL列表遮蔽的物理槽不计入，NULL列表项仍是一个
   出现项。关系只引用地址，不按值标识出现项，重复读数各自保留、不去重。
2. 三级目标。_match_group 用最小费用最大流保证"先最大化配对数、再最小化
   总绝对差"；并列最优解上按候选地址对（左地址、右地址）的字典序逐个强制，
   得到按左地址排序后字典序最小的地址对序列，与读数值次序无关。
3. 交付顺序与覆盖。plan_pairs 输出的 matches 按左地址升序，unmatched 左侧
   先于右侧、同侧地址升序；每侧配对地址与未配对地址互斥且恰好覆盖该侧全部
   出现项。decode_export 从自足来源重新推导出现项并核对这些不变量，无需
   重新求最优。

本文件的 reference_pairs 是独立穷举参照：枚举全部匹配后按 (-对数, 总差,
地址对序列) 取最小，不经过生产求解器。测试覆盖三级目标、读数与地址次序
不同、跨批次重复、空值与组别，并逐项核对派生行及覆盖。
"""
import itertools
import json
import unittest
from copy import deepcopy
from tempfile import TemporaryDirectory
from pathlib import Path
from columnlake.source.events import DataError
from columnlake.lake.publication import Lake
from columnlake.delivery.decoding import decode_export
from columnlake.delivery.pairing import Occurrence, plan_pairs


def reference_pairs(left_uvs, right_uvs, tolerance):
    """枚举全部匹配的独立参照：返回(配对索引对序列, 左未配对, 右未配对)。"""
    best = None
    chosen = []

    def visit(i, used_right):
        nonlocal best
        if i == len(left_uvs):
            sequence = list(chosen)
            key = (-len(sequence),
                   sum(abs(left_uvs[a] - right_uvs[b]) for a, b in sequence),
                   sequence)
            if best is None or key < best[0]:
                best = (key, sequence)
            return
        visit(i + 1, used_right)
        for j, value in enumerate(right_uvs):
            if j not in used_right and abs(left_uvs[i] - value) <= tolerance:
                used_right.add(j)
                chosen.append((i, j))
                visit(i + 1, used_right)
                chosen.pop()
                used_right.discard(j)

    visit(0, set())
    pairs = best[1]
    matched_left = {i for i, _ in pairs}
    matched_right = {j for _, j in pairs}
    return (pairs,
            [i for i in range(len(left_uvs)) if i not in matched_left],
            [j for j in range(len(right_uvs)) if j not in matched_right])


def group_side(batch_no, uvs):
    return [Occurrence((batch_no, 0, i), 1, 'g', uv, {'category': 'g', 'uv': uv})
            for i, uv in enumerate(uvs)]


def check_group(case, left_uvs, right_uvs, tolerance):
    matches, unmatched = plan_pairs(group_side(1, left_uvs), group_side(2, right_uvs), tolerance)
    expected_pairs, expected_left_rest, expected_right_rest = \
        reference_pairs(left_uvs, right_uvs, tolerance)
    case.assertEqual([(m[0][2], m[1][2]) for m in matches], expected_pairs)
    case.assertEqual([a[2] for s, a in unmatched if s == 'left'], expected_left_rest)
    case.assertEqual([a[2] for s, a in unmatched if s == 'right'], expected_right_rest)
    # 不变量：matches按左地址升序；unmatched左先右后、同侧地址升序。
    case.assertEqual(matches, sorted(matches, key=lambda m: m[0]))
    case.assertEqual(unmatched, sorted(unmatched, key=lambda e: (e[0] != 'left', e[1])))
    # 不变量：每侧配对地址与未配对地址互斥且恰好覆盖全部出现项。
    left_matched = [m[0] for m in matches]
    right_matched = [m[1] for m in matches]
    left_listed = [a for s, a in unmatched if s == 'left']
    right_listed = [a for s, a in unmatched if s == 'right']
    case.assertEqual(len(set(map(tuple, left_matched))), len(left_matched))
    case.assertEqual(len(set(map(tuple, right_matched))), len(right_matched))
    case.assertEqual(sorted(left_matched + left_listed), [[1, 0, i] for i in range(len(left_uvs))])
    case.assertEqual(sorted(right_matched + right_listed), [[2, 0, j] for j in range(len(right_uvs))])


class TestGroupExhaustive(unittest.TestCase):
    def test_all_small_groups(self):
        values = (0, 1, 2)
        domains = [()]
        for length in (1, 2, 3):
            domains.extend(itertools.product(values, repeat=length))
        checked = 0
        for left_uvs in domains:
            for right_uvs in domains:
                for tolerance in (0, 1, 2, 5):
                    check_group(self, list(left_uvs), list(right_uvs), tolerance)
                    checked += 1
        self.assertEqual(checked, len(domains) ** 2 * 4)

    def test_cardinality_beats_naive_greedy(self):
        # 按地址顺序贪心会先配 4→3，只剩一对；最优为两对，总差4。
        matches, unmatched = plan_pairs(group_side(1, [4, 0]), group_side(2, [3, 5]), 3)
        self.assertEqual(matches, [[[1, 0, 0], [2, 0, 1]], [[1, 0, 1], [2, 0, 0]]])
        self.assertEqual(unmatched, [])

    def test_address_tie_break_not_value_order(self):
        # 读数与地址次序不同：两种完美匹配总差均6，按地址字典序选首项对首项。
        matches, unmatched = plan_pairs(group_side(1, [0, 1]), group_side(2, [4, 3]), 4)
        self.assertEqual(matches, [[[1, 0, 0], [2, 0, 0]], [[1, 0, 1], [2, 0, 1]]])
        self.assertEqual(unmatched, [])

    def test_duplicates_stay_distinct(self):
        # 同值重复出现项各自保留并分别配对。
        matches, unmatched = plan_pairs(group_side(1, [5, 5]), group_side(2, [5, 5]), 0)
        self.assertEqual(matches, [[[1, 0, 0], [2, 0, 0]], [[1, 0, 1], [2, 0, 1]]])
        self.assertEqual(unmatched, [])


def scalar(values):
    return {'length': len(values), 'validity': [v is not None for v in values],
            'values': [0 if v is None else v for v in values]}


def make_events(batches):
    """batches: 每批为行列表；每行是 (sample, observations)，observations 为
    (category, uv) 列表、None（NULL列表）或 []（空列表）；项可为 None。"""
    events = [{'kind': 'schema', 'schema': [
        ['sample', 'int'],
        ['observations', {'list': {'struct': [['category', 'text'], ['uv', 'int']]}}]]}]
    for number, rows in enumerate(batches):
        flat = [item for _, observations in rows for item in (observations or [])]
        offsets = [0]
        for _, observations in rows:
            offsets.append(offsets[-1] + len(observations or []))
        items = {'length': len(flat),
                 'validity': [item is not None for item in flat],
                 'fields': {
                     'category': scalar([None if item is None else item[0] for item in flat]),
                     'uv': scalar([None if item is None else item[1] for item in flat])}}
        events.append({'kind': 'batch', 'batch_id': 'b' + str(number),
                       'row_count': len(rows),
                       'columns': {'sample': scalar([sample for sample, _ in rows]),
                                   'observations': {
                                       'length': len(rows),
                                       'validity': [obs is not None for _, obs in rows],
                                       'offsets': offsets, 'items': items}}})
    events.append({'kind': 'end'})
    return [dict(event, seq=i) for i, event in enumerate(events)]


class TestPairDeliveryEndToEnd(unittest.TestCase):
    def test_cross_batch_duplicates_and_nulls(self):
        left_batches = [
            [(1, [('g', 4), None, ('h', 2)])],   # batch_no 1：NULL列表项仍是出现项
            [(1, [('g', 4)]), (1, None), (None, [('g', 4)]), (1, [])],  # batch_no 2
        ]
        right_batches = [[(1, [('g', 4), ('g', 4)])]]  # batch_no 3：重复读数分别保留
        with TemporaryDirectory(prefix='columnlake-pair-test-') as directory:
            path = Path(directory) / 'pairs.sqlite'
            with Lake.open(path) as lake:
                lake.ingest('a', 'A', make_events(left_batches))
                receipt = lake.ingest('b', 'B', make_events(right_batches))
                snapshot = receipt.snapshot_id
                document = lake.export_pairs(snapshot, 'A', 'B', 0)
                self.assertEqual([m['batch_no'] for m in document['sources']['batches']], [1, 2, 3])
                expected = {
                    'pairs': [
                        {'left': [1, 0, 0], 'right': [3, 0, 0], 'sample': 1, 'category': 'g',
                         'left_uv': 4, 'right_uv': 4, 'delta_uv': 0},
                        {'left': [2, 0, 0], 'right': [3, 0, 1], 'sample': 1, 'category': 'g',
                         'left_uv': 4, 'right_uv': 4, 'delta_uv': 0}],
                    'unmatched': [
                        {'side': 'left', 'address': [1, 0, 1], 'sample': 1, 'observation': None},
                        {'side': 'left', 'address': [1, 0, 2], 'sample': 1,
                         'observation': {'category': 'h', 'uv': 2}},
                        {'side': 'left', 'address': [2, 2, 0], 'sample': None,
                         'observation': {'category': 'g', 'uv': 4}}]}
                # 关闭湖库后的独立解码：无连接、无来源查询。
            actual = decode_export(json.loads(json.dumps(document, ensure_ascii=False)))
            self.assertEqual(actual, expected)
            with Lake.open(path) as reopened:
                self.assertEqual(decode_export(reopened.export_pairs(snapshot, 'A', 'B', 0)), expected)

    def test_decoder_rejects_broken_relations(self):
        with TemporaryDirectory(prefix='columnlake-pair-test-') as directory:
            with Lake.open(Path(directory) / 'pairs.sqlite') as lake:
                lake.ingest('a', 'A', make_events([[(1, [('g', 4), ('h', 9), None])]]))
                receipt = lake.ingest('b', 'B', make_events([[(1, [('g', 6)])]]))
                document = lake.export_pairs(receipt.snapshot_id, 'A', 'B', 3)
                base = decode_export(json.dumps(document))
                self.assertEqual(len(base['pairs']), 1)
                self.assertEqual(len(base['unmatched']), 2)

                def broken(**changes):
                    candidate = deepcopy(document)
                    for key, value in changes.items():
                        candidate[key] = value
                    return candidate

                good_match = document['matches'][0]
                cases = [
                    # 重复消费同一左地址
                    broken(matches=[good_match, good_match]),
                    # 右地址越界：不是来源出现项
                    broken(matches=[[good_match[0], [2, 9, 0]]]),
                    # 容差收紧后已交付配对越限（|4-6|=2 > 1）
                    broken(tolerance=1),
                    # 右地址被两对重复消费
                    broken(matches=[good_match, [[1, 0, 1], [2, 0, 0]]]),
                    # 组别不符：左'h'项与右'g'项同组键不同
                    broken(matches=[[[1, 0, 1], [2, 0, 0]]],
                           unmatched=[['left', [1, 0, 0]], ['left', [1, 0, 2]]]),
                    # NULL列表项不具备配对资格
                    broken(matches=[[[1, 0, 2], [2, 0, 0]]],
                           unmatched=[['left', [1, 0, 0]], ['left', [1, 0, 1]]]),
                    # 覆盖不完整：丢掉全部未配对项
                    broken(unmatched=[]),
                    # 未配对项顺序错误
                    broken(unmatched=list(reversed(document['unmatched']))),
                    # 非法side
                    broken(unmatched=[['middle', document['unmatched'][0][1]]]),
                    # 左右归属错乱：配对左地址落在右侧来源
                    broken(left_stream='B', right_stream='A'),
                    # 顶层与来源快照不一致
                    broken(snapshot_id=receipt.snapshot_id + 1),
                ]
                for number, candidate in enumerate(cases):
                    with self.subTest(number=number), self.assertRaises(DataError):
                        decode_export(candidate)

    def test_decoder_checks_source_schema(self):
        with TemporaryDirectory(prefix='columnlake-pair-test-') as directory:
            with Lake.open(Path(directory) / 'bad.sqlite') as lake:
                receipt = lake.ingest('x', 'X', [
                    {'kind': 'schema', 'seq': 0, 'schema': [['sample', 'text']]},
                    {'kind': 'batch', 'seq': 1, 'batch_id': 'bad', 'row_count': 0,
                     'columns': {'sample': scalar([])}}])
                document = {'version': 2, 'snapshot_id': receipt.snapshot_id,
                            'left_stream': 'X', 'right_stream': 'Y', 'tolerance': 0,
                            'sources': lake.export(receipt.snapshot_id).to_obj(),
                            'matches': [], 'unmatched': []}
                with self.assertRaises(DataError):
                    decode_export(document)
                with self.assertRaises(DataError):
                    lake.export_pairs(receipt.snapshot_id, 'X', 'Y', 0)


if __name__ == '__main__':
    unittest.main()
