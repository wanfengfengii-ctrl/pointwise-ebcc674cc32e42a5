# ColumnLake format 1

所有文件和载荷使用UTF-8 JSON。规范JSON使用ensure_ascii=False、sort_keys=True、separators=(',', ':')、allow_nan=False。对象键顺序不表达业务顺序；字段、事件、行、列表和字典值的顺序由数组表达。无pickle。

## 输入类型与数组

schema为有序的`[[字段名, 类型], ...]`，字段名在同层唯一。类型为`int`、`text`、`{"struct": [[名, 类型], ...]}`、`{"list": 类型}`或`{"dict": {"id": 非负整数, "value": 类型}}`。

每个数组有精确整数length及同长度bool数组validity，并有以下载荷：

- int/text：同长度values。
- dict：同长度indices，目标编号来自类型树。
- struct：fields对象，字段集合与模式一致，子数组与父等长。
- list：offsets长度为length+1，从0开始、非递减、末尾等于items.length；items为子数组。

有效整数槽严格为int而非bool；有效文本槽严格为str。无效槽不解释其占位标量或索引，但所有数组结构仍校验。NULL父结构遮蔽其子值。NULL list和空list不同，NULL struct和字段为NULL也不同。

输入事件包含kind和从0开始连续的seq。schema有schema；dictionary有id、type、values及可省略的delta=false；batch有batch_id、row_count、columns；end没有额外载荷。dictionary替换和增量都产生新代；重复值不去重，增量连接旧序列和追加序列。字典类型在该编号的历史中固定。

## 冻结与湖段

内部GenerationKey为(stream_id, dictionary_id, generation)。FrozenReference直接绑定不可变代次和位置。值树中的struct和list使用不可变元组。批次携带完整引用闭包、原列结构的规范文本、有序绑定行和BatchAudit。

SQLite表为meta、streams、segments、batches、snapshots、members、receipts。meta.format为字符串1。空快照编号0。成功请求生成连续快照编号；批次编号从1按发布顺序增长。每个快照保留上一个快照成员的完整前缀。流状态、所有历史代次、批次、湖段、成员和回执共同提交。

一个湖段对应一个批次，载荷包含version=1、schema、row_count、columns和dictionaries。每个局部字典条目为`{"id": 局部编号, "type": 值类型, "array": 值数组}`。局部编号按完整GenerationKey排序分配，解释目录仅在该批次内有效。

持久dict数组具有length、validity、indices、references；references逐槽指向该批次的局部字典条目。逐槽引用允许同一追加字典的不同值绑定内层不同历史代次。有效槽的目标值类型须与类型树的value严格一致。无效槽不解引用。其他数组布局与输入一致。隐藏的子槽可以规范化为NULL或零长度区间；逻辑结果不变。

segments记录载荷规范文本、UTF-8字节长度、SHA-256和审计。读取先校验字节长度及SHA-256，再验证完整局部字典图、类型、索引、结构和审计。不能读取当前输入目录补全缺失解释。

## 独立核账

BatchAudit包含row_count、digest、leaf_values和nulls。完整逻辑行按schema字段顺序转为带类型树：NULL为`["null"]`；int为`["int", 十进制字符串]`；text为`["text", 原文本]`；struct为`["struct", [[字段名, 子标记], ...]]`；list为`["list", [子标记, ...]]`。字典按value类型标记其已解释值。

digest为规范JSON对象`{"format":1,"schema":原schema,"rows":[逐行标记]}`的UTF-8 SHA-256。leaf_values统计完整逻辑树中可达的非NULL int/text叶出现次数；nulls统计可达NULL节点，NULL祖先计一次且不展开子节点；空列表零项。这是整个批次的核账，不是某个profile路径的统计。

## 训练导出

ExportBundle为`{"version":1,"snapshot_id":编号,"batches":[...]}`。每个成员含batch_no、stream_id、batch_id、audit和encoded湖段对象，严格按批次编号递增。每个encoded拥有自己的局部字典目录。同号局部引用不跨成员共享。

独立decode_export验证成员身份和顺序、自足引用图、列结构、类型、索引及上述审计后返回逻辑行。它不调用SnapshotReader、lake.encoding.decode_batch或输入字典目录。布局和编号可以变化，行序、逻辑值、NULL、空列表和重复值必须与read一致。
