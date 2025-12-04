"""
完整链路追踪和统计模块

功能：
1. 从指定起始节点开始追踪所有完整链路
2. 链路可以分支（一个节点多个子节点），但每个节点只能有一个父节点
3. 如果节点有多个候选父节点，抛弃整条链路
4. 统计相同链路的数量（系统为节点，交易码为边）
"""

import pandas as pd
from typing import List, Dict, Set, Tuple
from collections import defaultdict, Counter
import time


class ChainTracer:
    """链路追踪器"""
    
    def __init__(self, df: pd.DataFrame, use_filtered: str = 'msg', discard_mode: str = 'branch'):
        """
        初始化链路追踪器
        
        参数:
            df: 包含候选节点列的 DataFrame
            use_filtered: 使用哪个过滤列 ('original', 'gid', 'msg')
            discard_mode: 抛弃模式
                - 'branch': 只抛弃该节点及其后续路径（默认，原规则）
                - 'chain': 抛弃整条链路（包括之前的链路）
        """
        self.df = df
        self.use_filtered = use_filtered
        self.discard_mode = discard_mode
        
        # 选择使用哪个候选列
        if use_filtered == 'msg':
            self.children_col = '候选子节点_msg过滤'
            self.parents_col = '候选父节点_msg过滤'
        elif use_filtered == 'gid':
            self.children_col = '候选子节点_gid过滤'
            self.parents_col = '候选父节点_gid过滤'
        else:
            self.children_col = '候选子节点'
            self.parents_col = '候选父节点'
        
        # 构建快速查找索引
        self._build_indexes()
    
    def _build_indexes(self):
        """构建索引：index -> row, index -> children, index -> parents（优化版本）"""
        print("构建索引...")
        
        # 使用 dict(zip()) 代替 iterrows()，快100倍
        indices = self.df['index'].values
        children_lists = self.df[self.children_col].values
        parents_lists = self.df[self.parents_col].values
        
        self.index_to_children = dict(zip(indices, children_lists))
        self.index_to_parents = dict(zip(indices, parents_lists))
        
        # 预先提取系统名和交易码（避免在递归中频繁访问 DataFrame）
        self.index_to_src_system = dict(zip(indices, self.df['srcSysname'].values))
        self.index_to_dst_system = dict(zip(indices, self.df['dstSysname'].values))
        self.index_to_msgtype = dict(zip(indices, self.df['msgType'].values))
        
        # 预先构建 index -> row_data 映射（优化性能）
        print("构建行数据映射...")
        all_records = self.df.to_dict('records')
        self.index_to_row = {record['index']: record for record in all_records}
        
        print("索引构建完成")
    
    def find_chains_from_root(
        self, 
        root_conditions: List[Tuple[str, str]],
        max_depth: int = 100
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        从指定起始节点查找所有有效的完整链路
        
        参数:
            root_conditions: 起始节点条件列表 [(系统名, 交易码), ...]
            max_depth: 最大深度（防止环路）
        
        返回:
            (valid_chains, stats)
            - valid_chains: 有效链路列表，每条链路是 [(系统, 交易码), ...] 的列表
            - stats: 统计信息字典
        """
        # print(f"\n{'='*80}")
        # print(f"开始追踪链路，起始节点条件：")
        # for system, msg_type in root_conditions:
        #     print(f"  - ({system}, {msg_type})")
        # print(f"{'='*80}")
        
        start_time = time.time()
        
        # 查找所有符合条件的起始节点，同时记录起始条件
        root_nodes_list = []
        root_conditions_map = {}  # index -> (系统名, 交易码)
        # print(f"\n每个条件找到的起始节点：")
        for root_system, root_msg_type in root_conditions:
            nodes = self.df[
                (self.df['srcSysname'] == root_system + '-F5') &
                (self.df['msgType'] == root_msg_type)
            ]
            # print(f"  ({root_system}, {root_msg_type}): {len(nodes)} 个")
            
            # 记录每个节点的起始条件
            for node_idx in nodes['index'].values:
                root_conditions_map[node_idx] = (root_system, root_msg_type)
            
            root_nodes_list.append(nodes)
        
        # 合并所有起始节点
        if root_nodes_list:
            root_nodes = pd.concat(root_nodes_list, ignore_index=True)
        else:
            root_nodes = pd.DataFrame()
        
        print(f"\n总计: {len(root_nodes)} 个起始节点")
        
        if len(root_nodes) == 0:
            print("警告：没有找到符合条件的起始节点")
            return pd.DataFrame(), {}
        
        all_chain_dfs = []
        discarded_count = 0
        
        # 对每个起始节点进行追踪
        print(f"\n开始追踪...")
        n_roots = len(root_nodes)
        print_interval = max(1, n_roots // 10)  # 每10%显示一次
        
        # 使用 values 避免 iterrows()
        root_indices = root_nodes['index'].values
        
        for idx, root_idx in enumerate(root_indices):
            if (idx + 1) % print_interval == 0 or (idx + 1) == n_roots:
                print(f"  进度: {idx + 1}/{n_roots} ({(idx + 1)*100//n_roots}%)")
            
            # 获取该节点的起始条件
            root_condition = root_conditions_map.get(root_idx, ('UNKNOWN', 'UNKNOWN'))
            
            # 使用 DFS 追踪所有路径，返回 index -> depth 映射
            # 每个起始节点分配一个 trace_id
            index_to_depth, discarded, chain_discarded = self._trace_from_node(
                root_idx, 
                path_indices=[], 
                visited=set(),
                max_depth=max_depth,
                trace_id=idx,  # 0, 1, 2, ...
                root_condition=root_condition
            )
            
            # chain_discarded=True 表示整条链路被抛弃（新规则）
            if chain_discarded:
                discarded_count += 1
                continue  # 跳过这条链路，不添加到结果中
            
            # 如果找到了链路，构建 DataFrame
            if index_to_depth:
                # 构建该链路的 DataFrame
                rows = []
                for idx_val, depth in index_to_depth.items():
                    row_data = self.index_to_row[idx_val].copy()
                    row_data['trace_id'] = idx
                    row_data['depth'] = depth
                    if root_condition:
                        row_data['root_system'] = root_condition[0]
                        row_data['root_msgType'] = root_condition[1]
                    rows.append(row_data)
                
                chain_df = pd.DataFrame(rows)
                all_chain_dfs.append(chain_df)
            
            discarded_count += discarded
        
        elapsed = time.time() - start_time
        
        # 合并所有链路 DataFrame
        if all_chain_dfs:
            all_chains_df = pd.concat(all_chain_dfs, ignore_index=True)
            n_unique_chains_before = all_chains_df['trace_id'].nunique()
            
            # 检查每个 trace_id 的 global_id 情况
            valid_trace_ids = []
            for trace_id in all_chains_df['trace_id'].unique():
                trace_data = all_chains_df[all_chains_df['trace_id'] == trace_id]
                # 获取所有非 None 的 global_id
                global_ids = trace_data['global_id'].dropna().unique()
                
                # 如果 global_id 种类不超过 1 种，保留这个链路
                if len(global_ids) <= 1:
                    valid_trace_ids.append(trace_id)
            
            # 过滤掉 global_id 超过一种的链路
            if valid_trace_ids:
                all_chains_df = all_chains_df[all_chains_df['trace_id'].isin(valid_trace_ids)]
                n_unique_chains = len(valid_trace_ids)
            else:
                all_chains_df = pd.DataFrame()
                n_unique_chains = 0
            
            # 统计被过滤的链路数
            filtered_count = n_unique_chains_before - n_unique_chains
            if filtered_count > 0:
                print(f"\n过滤掉 {filtered_count} 条链路（global_id 超过一种）")
        else:
            all_chains_df = pd.DataFrame()
            n_unique_chains = 0
            filtered_count = 0
        
        # 统计信息
        total_paths = n_unique_chains + discarded_count + (filtered_count if all_chain_dfs else 0)
        discard_mode_desc = '整条链路' if self.discard_mode == 'chain' else '分支路径'
        stats = {
            '起始节点数量': len(root_nodes),
            '有效链路数量': n_unique_chains,
            '被抛弃的路径': discarded_count,
            '过滤的链路（global_id>1种）': filtered_count if all_chain_dfs else 0,
            '总路径数': total_paths,
            '抛弃率': f"{(discarded_count + (filtered_count if all_chain_dfs else 0))/total_paths*100:.2f}%" if total_paths > 0 else "0%",
            '抛弃模式': discard_mode_desc,
            '耗时': f"{elapsed:.2f}秒"
        }
        
        print(f"\n追踪完成（抛弃模式: {discard_mode_desc}）：")
        print(f"  - 有效链路: {stats['有效链路数量']}")
        print(f"  - 被抛弃: {stats['被抛弃的路径']}")
        print(f"  - 过滤（global_id>1种）: {stats['过滤的链路（global_id>1种）']}")
        print(f"  - 抛弃率: {stats['抛弃率']}")
        print(f"  - 耗时: {stats['耗时']}")
        
        return all_chains_df, stats
    
    def _trace_from_node(
        self, 
        node_idx: int, 
        path_indices: List[int],
        visited: Set[int],
        max_depth: int,
        trace_id: int = 0,
        root_condition: Tuple[str, str] = None
    ) -> Tuple[Dict[int, int], int, bool]:
        """
        从指定节点递归追踪链路（DFS），返回所有节点的 index -> depth 映射
        
        参数:
            node_idx: 当前节点的 index
            path_indices: 当前路径的 index 列表
            visited: 已访问的节点集合（防止环路）
            max_depth: 最大深度
            trace_id: 链路追踪ID（整数）
            root_condition: 起始条件 (系统名, 交易码)
        
        返回:
            (index_to_depth, discarded_count, chain_discarded)
            - index_to_depth: 该链路树中所有节点的 {index: depth} 映射
            - discarded_count: 被抛弃的路径数量
            - chain_discarded: 是否整条链路被抛弃（新规则）
        """
        # 防止环路
        if node_idx in visited:
            return {}, 0, False
        
        # 防止过深
        if len(path_indices) >= max_depth:
            return {}, 0, False
        
        # 检查节点是否存在
        if node_idx not in self.index_to_src_system:
            return {}, 0, False
        
        # 添加当前节点到路径
        new_path_indices = path_indices + [node_idx]
        new_visited = visited | {node_idx}
        
        # 获取子节点
        children = self.index_to_children.get(node_idx, [])
        
        # 收集当前路径的所有节点及其 depth
        index_to_depth = {idx: depth for depth, idx in enumerate(new_path_indices, 1)}
        
        # 如果没有子节点，返回当前路径
        if len(children) == 0:
            return index_to_depth, 0, False
        
        # 递归追踪所有子节点
        total_discarded = 0
        
        for child_idx in children:
            # 检查子节点是否有多个候选父节点
            child_parents = self.index_to_parents.get(child_idx, [])
            
            # 关键逻辑：如果子节点有多个候选父节点
            if len(child_parents) > 1:
                if self.discard_mode == 'chain':
                    # 新规则：抛弃整条链路（包括之前的链路）
                    return {}, 0, True
                else:
                    # 原规则：只抛弃该节点及其后续路径
                    total_discarded += 1
                    continue
            
            # 递归追踪
            child_index_to_depth, child_discarded, child_chain_discarded = self._trace_from_node(
                child_idx,
                new_path_indices,
                new_visited,
                max_depth,
                trace_id,
                root_condition
            )
            
            # 如果子节点返回整条链路被抛弃，直接返回
            if child_chain_discarded:
                return {}, 0, True
            
            # 合并子节点的 index_to_depth（保留最小的 depth，即最早出现的路径）
            for idx, depth in child_index_to_depth.items():
                if idx not in index_to_depth or depth < index_to_depth[idx]:
                    index_to_depth[idx] = depth
            
            total_discarded += child_discarded
        
        return index_to_depth, total_discarded, False
    
    def extract_single_chain_graph(self, chains_df: pd.DataFrame, trace_id: int) -> Dict:
        """
        从单个链路提取图结构
        
        参数:
            chains_df: 包含 trace_id 和 depth 的链路 DataFrame
            trace_id: 要提取的链路ID
        
        返回:
            图结构字典，包含:
            - nodes: 节点列表 [node_name, ...]
            - edges: 边列表 [{'source': src, 'target': dst, 'msgType': msg}, ...]
        """
        # 筛选指定 trace_id 的数据
        chain_data = chains_df[chains_df['trace_id'] == trace_id]
        
        if chain_data.empty:
            return {'nodes': [], 'edges': []}
        
        nodes = set()
        edges_set = set()  # 使用集合去重
        
        # 遍历每行数据，提取节点和边
        for i, row in chain_data.iterrows():
            src = row['srcSysname']
            dst = row['dstSysname']
            msg_type = row['msgType']
            
            # 添加节点
            nodes.add(src)
            nodes.add(dst)
            
            # 添加边（使用元组去重）
            edges_set.add((src, dst, msg_type))
        
        # 转换为列表格式
        edges = [
            {'source': src, 'target': dst, 'msgType': msg}
            for src, dst, msg in edges_set
        ]
        
        return {
            'nodes': sorted(list(nodes)),  # 排序后返回列表
            'edges': edges
        }
    
    def count_graph_structures(self, chains_df: pd.DataFrame) -> pd.DataFrame:
        """
        统计每种图结构的数量
        
        参数:
            chains_df: 包含 trace_id 的链路 DataFrame
        
        返回:
            统计 DataFrame，包含图结构和数量
        """
        if chains_df.empty:
            return pd.DataFrame()
        
        from collections import Counter, defaultdict
        
        # 为每个 trace_id 提取图结构
        graph_structures = {}
        
        for trace_id in chains_df['trace_id'].unique():
            graph = self.extract_single_chain_graph(chains_df, trace_id)
            
            # 将图结构转换为可哈希的字符串（用于比较）
            # 节点已经排序
            nodes_str = ",".join(graph['nodes'])
            
            # 对边排序（按 source, target, msgType）
            edges_sorted = sorted(graph['edges'], key=lambda x: (x['source'], x['target'], x['msgType']))
            edges_str = ";".join([f"{e['source']}->{e['target']}[{e['msgType']}]" for e in edges_sorted])
            
            # 组合成唯一标识
            graph_signature = f"Nodes:{nodes_str}|Edges:{edges_str}"
            
            graph_structures[trace_id] = {
                'signature': graph_signature,
                'graph': graph
            }
        
        # 统计每种图结构的数量，并记录所有对应的 trace_id
        signature_to_trace_ids = defaultdict(list)
        for trace_id, data in graph_structures.items():
            signature_to_trace_ids[data['signature']].append(trace_id)
        
        # 构建结果 DataFrame
        results = []
        graph_id = 0  # 从 0 开始的连续编号
        for signature, trace_ids in sorted(signature_to_trace_ids.items(), key=lambda x: len(x[1]), reverse=True):
            # 获取图结构示例
            example_trace_id = trace_ids[0]
            example_graph = graph_structures[example_trace_id]['graph']
            
            # 收集所有 trace_id 对应的起始条件
            root_conditions = set()
            for tid in trace_ids:
                trace_data = chains_df[chains_df['trace_id'] == tid]
                if not trace_data.empty and 'root_system' in trace_data.columns and 'root_msgType' in trace_data.columns:
                    root_sys = trace_data.iloc[0]['root_system']
                    root_msg = trace_data.iloc[0]['root_msgType']
                    if pd.notna(root_sys) and pd.notna(root_msg):
                        root_conditions.add((root_sys, root_msg))
            
            # 转换为列表格式
            root_conditions_list = sorted(list(root_conditions))
            
            results.append({
                'graph_id': graph_id,  # 添加图结构编号
                '图结构': signature,
                '链路数量': len(trace_ids),
                '节点数': len(example_graph['nodes']),
                '边数': len(example_graph['edges']),
                'trace_id': [int(tid) for tid in trace_ids],  # 转换为普通 Python int 列表
                'root_conditions': root_conditions_list,  # 所有起始条件
                '节点': example_graph['nodes'],
                '边': example_graph['edges']
            })
            graph_id += 1  # 递增编号
        
        result_df = pd.DataFrame(results)
        
        print(f"\n发现 {len(result_df)} 种不同的图结构")
        print(f"总计 {len(graph_structures)} 条链路")
        
        return result_df


def trace_and_analyze(
    df: pd.DataFrame,
    root_conditions,  # 可以是 List[Tuple[str, str]] 或单个 (str, str)
    use_filtered: str = 'msg',
    max_depth: int = 100,
    output_prefix: str = None,
    merge_with_input: bool = False,
    discard_mode: str = 'branch'
) -> Tuple[pd.DataFrame, Dict, pd.DataFrame]:
    """
    一键追踪和分析链路
    
    参数:
        df: 包含候选节点列的 DataFrame
        root_conditions: 起始节点条件: [('系统名1', '交易码1'), ('系统名2', '交易码2'), ...]
        use_filtered: 使用哪个过滤列 ('original', 'gid', 'msg')
        max_depth: 最大深度
        output_prefix: 输出文件前缀（可选）
        merge_with_input: 是否将结果与输入 df 合并，保留所有原始数据（默认 False）
        discard_mode: 抛弃模式
            - 'branch': 只抛弃该节点及其后续路径（默认，原规则）
            - 'chain': 抛弃整条链路（包括之前的链路）
    
    返回:
        (chains_df, stats, graph_stats_df)
        - chains_df: 完整链路数据 DataFrame，包含 trace_id 和 depth
                    如果 merge_with_input=True，则包含所有输入 df 的行
        - stats: 统计信息字典
        - graph_stats_df: 图结构统计 DataFrame
    
    示例:
        # 单个起始条件
        trace_and_analyze(df, ('SCS', 'scs.003.001'))
        
        # 多个起始条件，合并结果
        trace_and_analyze(df, [('SCS', 'scs.003.001'), ('SSS', 'sss.002.001')], merge_with_input=True)
        
        # 使用新规则：整条链路抛弃
        trace_and_analyze(df, root_conditions, discard_mode='chain')
    """
    # 创建追踪器
    tracer = ChainTracer(df, use_filtered=use_filtered, discard_mode=discard_mode)
    
    # 追踪链路，返回 DataFrame
    chains_df, stats = tracer.find_chains_from_root(
        root_conditions=root_conditions,
        max_depth=max_depth
    )
    
    if chains_df.empty:
        print("\n没有找到有效链路")
        return pd.DataFrame(), stats, pd.DataFrame()
    
    # 统计图结构
    graph_stats_df = tracer.count_graph_structures(chains_df)
    
    # 检查：除起始点外，候选父节点_msg过滤 的值的长度是否有大于1的情况
    if '候选父节点_msg过滤' in chains_df.columns and 'depth' in chains_df.columns:
        non_root_nodes = chains_df[chains_df['depth'] > 1]
        if not non_root_nodes.empty:
            multi_parent_count = non_root_nodes[
                non_root_nodes['候选父节点_msg过滤'].apply(lambda x: len(x) > 1)
            ]
            if not multi_parent_count.empty:
                print(f"\n⚠️ 警告：发现 {len(multi_parent_count)} 个非起始节点有多个候选父节点（msg过滤后）")
                print("示例节点：")
                print(multi_parent_count[['index', 'trace_id', 'depth', 'srcSysname', 'dstSysname', 'msgType', '候选父节点_msg过滤']].head(5))
            else:
                print("\n✓ 检查通过：所有非起始节点的候选父节点_msg过滤数量 ≤ 1")
        else:
            print("\n⚠️ 注意：链路中只有起始节点")

    # 保存结果
    if output_prefix:
        # 保存完整的链路数据
        chain_file = f"{output_prefix}_chains.csv"
        chains_df.to_csv('result/' + chain_file, index=False, encoding='utf-8-sig')
        print(f"\n完整链路数据已保存到: {chain_file}")
        
        # 保存图结构统计
        if not graph_stats_df.empty:
            graph_stats_file = f"{output_prefix}_graph_stats.csv"
            # 保存时排除复杂字段（节点和边的列表）
            save_cols = []
            if 'graph_id' in graph_stats_df.columns:
                save_cols.append('graph_id')
            save_cols.extend(['图结构', '链路数量', '节点数', '边数', 'trace_id'])
            if 'root_conditions' in graph_stats_df.columns:
                save_cols.append('root_conditions')
            graph_stats_simple = graph_stats_df[save_cols].copy()
            graph_stats_simple.to_csv('result/' + graph_stats_file, index=False, encoding='utf-8-sig')
            print(f"图结构统计已保存到: {graph_stats_file}")

    # 如果需要与输入合并
    if merge_with_input:
        # 获取链路特定的列
        chain_specific_cols = ['trace_id', 'depth']
        if 'root_system' in chains_df.columns:
            chain_specific_cols.append('root_system')
        if 'root_msgType' in chains_df.columns:
            chain_specific_cols.append('root_msgType')

        # 提取链路信息
        chain_info = chains_df[['index'] + chain_specific_cols].copy()

        # 与原始 df 进行左连接
        merged_df = df.merge(chain_info, on='index', how='left', suffixes=('', '_chain'))
        chains_df = merged_df

        print(f"\n结果已与输入 df 合并，保留所有 {len(df)} 行数据")
        print(f"其中 {chain_info['index'].nunique()} 行在链路中")

    return chains_df, stats, graph_stats_df


# 使用示例
if __name__ == "__main__":
    import sys
    sys.path.append('.')
    from chain_transaction_new import chain_df_heap
    
    # 读取数据
    print("读取数据...")
    df = pd.read_csv("result/PSISADP_test_1117.csv")
    df['end_at_ms'] = df['start_at_ms'] + df["latency_msec"]
    df = df.sort_values(by='start_at_ms').reset_index(drop=True).iloc[:100000]
    df['index'] = df.index
    
    # 执行串联
    print("执行串联...")
    result = chain_df_heap(df.copy(), left_ms=0, right_ms=0)
    
    # 追踪和分析链路
    
    # 从文本文件读取起始条件
    root_conditions_file = 'root_conditions.txt'
    print(f"从文件读取起始条件: {root_conditions_file}")
    
    root_conditions = []
    with open(root_conditions_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # 跳过空行和注释
                parts = line.split()
                if len(parts) >= 2:
                    root_conditions.append((parts[0], parts[1]))
    
    print(f"加载了 {len(root_conditions)} 个起始条件")
    
    chains_df, stats, graph_stats_df = trace_and_analyze(
        df=result,
        root_conditions=root_conditions,
        use_filtered='msg',  # 使用 msgType 过滤后的结果
        output_prefix='chain_analysis'
    )