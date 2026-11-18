# 固定快照的两仪器对照交付

ColumnLake V1的接纳、固定读取、profile、原样训练导出与独立解码已完整提供，接口和数组输入见README.md及FORMAT.md。本题只增加配对加工与离线关系解释。pair_examples.py提供正常V1可接纳的输入及单独手算结果，没有配对求解器。

## 外部入口与选取范围

新增Lake.export_pairs(snapshot_id, left_stream, right_stream, tolerance)，返回普通JSON对象；原Lake.export的ExportBundle行为不变。decode_export继续接受V1对象、JSON文本和ExportBundle；对新对象或其JSON文本返回下述派生行。

snapshot_id使用V1的非布尔非负整数规则，必须存在。left_stream和right_stream均为非空字符串且不同；tolerance为非布尔非负整数，不限制整数位数。选取指定快照中这两流的全部批次，保留原全局batch_no，不重新编号。快照中没有某流批次时该侧为空；空快照也可交付。

每个选中批次必须具备sample:int和observations:list<struct<category:text,uv:int>>的逻辑类型；字段次序不限，允许额外字段以及任意位置的字典包装，按FORMAT.md解释后的类型判断，不能靠当前数据值猜类型；零行批次仍检查类型。额外字段不参与配对；未配对项保留完整的原观测struct，包括额外字段。只检查所选流的模式，不要求其他流模式相容。

## 出现项与唯一结果

观测地址为[batch_no, 零起行号, 零起列表项号]，按三个整数逐项比较。NULL列表和空列表没有出现项；NULL列表项仍是一个出现项。读取逻辑值后计算地址，不能把被NULL列表遮蔽的物理槽计入出现项。

sample、category、uv均非NULL的项才可配对，组键为(sample,category)；只有两项同组且绝对微伏差不超过tolerance才可连边。整数精确运算，文本不归一化，重复值仍是不同出现项。每组先最大化配对数，再最小化总绝对差，最后选择按左地址排序的地址对序列中字典序最小者。序列的元素是[左地址,右地址]；不按读数排序来决定并列结果。

每项至多使用一次；其余项全部进入unmatched，包括不具备配对资格的项。pairs按左地址排序，unmatched按左侧先于右侧、同侧地址升序排列。每侧的配对地址与未配对地址必须互斥且恰好覆盖该侧全部出现项。允许全量读取、物化和重算，不要求增量算法、持久任务、并发或资源配额。

## 自足交付与解码结果

新交付的顶层字段为：

- version：整数2。
- snapshot_id、left_stream、right_stream、tolerance：此次调用的固定参数。
- sources：FORMAT.md中的完整V1 ExportBundle对象，仅包含所选两流的全部批次，仍按batch_no递增；其snapshot_id必须等于顶层编号，每个成员保留原身份、审计和自足encoded。
- matches：按左地址升序排列的[左地址,右地址]列表。
- unmatched：按上述顺序排列的[side,地址]列表，side只能为"left"或"right"。

这是关系和来源的交付，不把未校验的派生值作为事实来源。以下为空快照的完整新交付：

```json
{"version":2,"snapshot_id":0,"left_stream":"A","right_stream":"B","tolerance":0,"sources":{"version":1,"snapshot_id":0,"batches":[]},"matches":[],"unmatched":[]}
```

独立decode_export须先验证来源的V1结构和审计，再解释关系；无需数据库、Lake连接、当前输入字典或主湖段解码器。它须检查参数、来源流身份与顺序、地址整数及边界、左右归属、输出顺序、组别、容差、无重复消费及完整覆盖；不要求解码器重新求最优。完整覆盖针对包内全部来源出现项，导出端负责保证来源包含指定快照所选流的全部批次；不要求离线包证明数据库之外的来源真实性。

新版本解码返回普通对象{"pairs":配对行列表,"unmatched":未配对行列表}。配对行字段为left、right、sample、category、left_uv、right_uv、delta_uv；left/right为地址，delta_uv精确等于right_uv-left_uv。未配对行字段为side、address、sample、observation；observation为完整原观测对象或None。NULL样本仍保留为None。不得从相同值反猜来源或按值去重。

未知快照、非法参数、不相容模式和非法交付抛DataError；必要湖段损坏沿用StorageError。导出和独立解码都须先完成本次校验，再返回完整结果，失败不返回部分交付。不得修改湖中原批次、回执或快照。新接纳、用户修改已返回对象或关闭重开不能改变固定快照的含义；V1全部接口及错误行为保持。

## 业务样例与交付核验

同组A按地址为[4,0]、B为[3,5]、容差3，应配4→5、0→3，两对总绝对差4。A=[0,4]、B=[1,3,8]、容差10，应配0→1、4→3，B的8未配对。A=[0,1]、B=[4,3]、容差4，两种完美匹配总差均6，地址并列规则选A首项→B首项、A次项→B次项。两侧各两项同值时仍交付两对。完整输入和派生行手算结果见pair_examples.py的cases()。

保留全部原测试及断言。补独立小组穷举参照，覆盖三级目标、读数与地址次序不同、跨批次重复、空值与组别，并逐项核对派生行及覆盖；说明生产表示如何维持这些不变量。

在项目根目录可执行：

```sh
python3 -B pair_examples.py --fixtures
python3 -B -m unittest discover -s tests -p 'test_*.py'
python3 -B -m columnlake.lake.publication --demo
```

--fixtures仅核对样例经V1接纳和原样导出的真实值，可在开发前运行。实现新能力后执行python3 -B pair_examples.py --verify；它保留原完整回归并核对手算配对、离线解码、非法关系拒绝及固定快照重开，不代替自行补充的独立穷举核账。python3 -B pair_examples.py --demo演示贪心少配样例、关闭湖库后解码和重开配对。任何失败均保留非零退出码。

沿用.pointwise-runtime.json的现有清单格式，将verify接通全部原回归、配对联合核验及补充参照，将demo接通真实配对交付演示；准备仍无需外部服务。配对规划产出的地址关系须由训练交付与独立解码共同消费。
