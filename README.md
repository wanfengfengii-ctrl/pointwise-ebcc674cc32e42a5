# ColumnLake

澄源研究所的列式实验数据湖，使用Python 3.11标准库和SQLite。上游提供已解帧的确定性事件；下游使用固定快照复核和自足训练导出。

## 运行

在项目根目录执行：

```sh
python3 -B -m unittest discover -s tests -p 'test_*.py'
python3 -B -m columnlake.lake.publication --demo
python3 -B pair_examples.py --verify
python3 -B pair_examples.py --demo
```

无需安装依赖。测试和demo只在各自临时目录创建数据库。demo读取项目中的有限输入及单独手算预期，断言查询、类别统计、独立训练解码、重开和重放结果，再打印POINTWISE_SCENARIO JSON。它不修改已有用户库。pair_examples.py的--verify保留完整回归并核对手算配对、离线解码、非法关系拒绝与固定快照重开；--demo演示贪心少配样例、关闭湖库后解码和重开配对。

## 一天的业务

1. 工程师打开指定SQLite文件；首次打开建立空库，重开保留已提交成果。
2. 为仪器流提交具有唯一request_id的完整有序事件。
3. schema固定类型和字段顺序；字典替换或追加产生不可变代次。
4. 整理整数微伏、类别、struct和list列，保留行序、NULL、空列表及重复项。
5. 完整消费输入后，在一个事务内发布湖段、目录、流进度、快照和回执。
6. 质控员用固定snapshot_id读取完整行或嵌套投影。
7. 逐次统计类别与NULL，并与独立批次摘要核对。
8. 分析员导出同一快照，在脱离数据库的环境中独立解码；关闭重开不改变结果。

## 公开API

```python
from columnlake.lake.publication import Lake
from columnlake.delivery.decoding import decode_export

with Lake.open('research.sqlite') as lake:
    receipt = lake.ingest('request-001', 'instrument-A', events)
    rows = lake.read(receipt.snapshot_id)
    categories = lake.read(receipt.snapshot_id, 'observations.category')
    counts = lake.profile(receipt.snapshot_id, 'observations.category').to_obj()
    training = lake.export(receipt.snapshot_id).to_obj()
assert decode_export(training) == rows
```

数据库文件的父目录须已存在。输入使用sample-events.json中的对象，或source.events中的SchemaEvent、DictionaryEvent、BatchEvent、EndEvent。SchemaEvent(seq, schema)建立模式；DictionaryEvent(seq, dictionary_id, value_type, values, delta=False)定义列式字典值；BatchEvent(seq, batch_id, columns, row_count)提交批次；EndEvent(seq)永久关闭流。每流序号从0连续增长。活动流可跨多个请求续接；schema只能建立一次。正常耗尽即允许提交，无须每个请求都有end。

请求身份包含stream_id及全部有序规范事件。相同request_id、相同内容返回原IntakeReceipt，不新增快照；任一内容变化拒绝。即使原流已经end，重放仍先完整消费输入并核对内容。源异常对象原样传播。batch_id在流内唯一。

read(snapshot_id, projection=None)返回按批次及行顺序排列的行。投影可使用点分路径或字段名序列；列表层次和NULL祖先保留。非法路径及未知快照使用DataError，不回退最新快照。空快照可完整读取或导出，但没有可解析的投影模式。

profile只接受最终为int或text的路径，返回CategoryProfile。counts按首次出现顺序排列，包含type、value和精确count；nulls单列。重复项逐次计数，空列表零项，NULL列表、NULL祖先或NULL叶沿可达分支贡献一个NULL。不同类型的类别分开统计。

export返回ExportBundle，可调用to_obj或取payload JSON文本。decode_export只消费该对象或JSON，不持有连接、不调用主湖段解码器。编号和布局属于局部编码，不能跨批次直接合并字典目录。

export_pairs(snapshot_id, left_stream, right_stream, tolerance)导出同一固定快照中两仪器流的对照训练交付：普通JSON对象，含version=2、调用参数、仅含所选两流批次的自足V1 sources、按左地址升序的matches及左先右后的unmatched。同一样本和类别、微伏差不超容差才可一对一配对；每组依次最大化对数、最小化总绝对差，再按来源地址取字典序最小关系。decode_export对新对象或其JSON文本独立核对来源审计、地址、组别、容差、无重复消费及完整覆盖后返回派生行，无需重新求最优。规则细节见PAIRS.md。

## 类型、错误和持久边界

类型语法及数组见FORMAT.md。整数不限SQLite有符号64位范围，bool不冒充int；文本不归一化，不换算微伏，不进行浮点舍入。字典值自身可含字典编码，引用只能指向事件发生时已存在的相容代次；不可前向引用或形成循环。替换、增量和其他流复用编号均不改变旧批次。

DataError表示非法输入、结构、字典引用、请求冲突、路径或快照。StorageError表示库格式、目录或湖段损坏。已有文件不被当作新库重置。直接修改内部SQLite不是支持的更新接口；正常用户通过ingest新增成果。载荷校验、解码或审计失败时read和export明确报错，不生成替代数据。

Lake.open(path, fault=callback)提供确定性发布故障点：after_segments、after_catalog、after_snapshot、after_receipt。回调抛出的异常使当前事务全部回滚。测试使用真实写入及回滚，不替换数据库实现。

source负责游标、请求身份及候选；dictionary负责不可变代次和传递引用；assembly负责结构、逻辑批次及摘要；lake负责事务、目录和局部编码；delivery负责固定读取、投影、次数归约及独立解码。模式演进、并发接纳、二进制Arrow IPC、压实和远端服务不在V1范围。
