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
from collections import defaultdict, Counter, deque
import time


class ChainTracer:
    """链路追踪器"""
    
    def __init__(self, df: pd.DataFrame, use_filtered: str = 'msg', discard_mode: str = 'branch'):
        """
        初始化链路追踪器
        
        参数:
            df: 包含候选节点列的 DataFrame
            use_filtered: 使用哪个过滤列 ('original', 'gid', 'msg')
                - 'msg': 使用同时满足 global_id 与 ESB/msgType 规则的候选关系
                        （列：候选子节点_gid_esb过滤 / 候选父节点_gid_esb过滤）
            discard_mode: 抛弃模式
                - 'branch': 只抛弃该节点及其后续路径（默认，原规则）
                - 'chain': 抛弃整条链路（包括之前的链路）
        """
        self.df = df
        self.use_filtered = use_filtered
        self.discard_mode = discard_mode
        
        # 选择使用哪个候选列
        if use_filtered == 'msg':
            # 默认：使用 gid 过滤与 ESB/msgType 规则的交集结果
            self.children_col = '候选子节点_gid_esb过滤'
            self.parents_col = '候选父节点_gid_esb过滤'
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
    
    def trace_bidirectional(self, start_node_idx: int, max_depth: int = 100) -> List[int]:
        """
        双向追踪核心算法 (BFS)，不使用 self.index_to_gid 结构，
        直接从 self.index_to_row 中获取 Global ID 并判断唯一性。
        当 max_depth 为 -1 时，不限制搜索深度。
        
        参数:
            start_node_idx: 起始节点 index
            max_depth: 最大深度，-1 表示无限制
        
        返回:
            component_nodes: 连通组件中的所有节点 index 列表，如果链路无效则返回 None
        """
        # 队列中存储 (节点 index, 当前跳数)
        queue = deque([(start_node_idx, 0)])
        component_nodes = []
        seen_indices = {start_node_idx: 0}
        seen_gids = set()  # 收集 Global ID

        # 预先设定不限制深度标志
        no_depth_limit = (max_depth == -1)

        # --- 初始节点检查 ---
        start_row = self.index_to_row.get(start_node_idx)
        if not start_row:
            return None  # 起始节点不存在
        
        # 检查起始节点的父节点数量（如果起始节点有多个父节点，整条链路作废）
        start_parents = self.index_to_parents.get(start_node_idx, [])
        if len(start_parents) > 1:
            return None  # 起始节点有多个父节点，整条链路作废
        
        start_gid = start_row.get('global_id')
        if pd.notna(start_gid) and start_gid != '':
            seen_gids.add(start_gid)

        while queue:
            curr_idx, curr_hop = queue.popleft()

            # 深度限制检查 (仅在 no_depth_limit 为 False 时执行)
            if not no_depth_limit and curr_hop >= max_depth:
                continue

            # 将当前节点加入链路（父节点数量已在加入队列前检查过）
            component_nodes.append(curr_idx)

            # --- 扩展邻居 (双向) ---
            children = self.index_to_children.get(curr_idx, [])
            parents = self.index_to_parents.get(curr_idx, [])
            
            # 合并邻居节点，先处理子节点，再处理父节点
            neighbors = list(children) + list(parents)
            next_hop = curr_hop + 1

            # 下一跳深度限制检查 (仅在 no_depth_limit 为 False 时执行)
            should_continue = no_depth_limit or (next_hop < max_depth)

            for neighbor_idx in neighbors:
                # 跳过已访问的节点
                if neighbor_idx in seen_indices:
                    continue
                
                # 深度限制检查
                if not should_continue:
                    continue

                neighbor_row = self.index_to_row.get(neighbor_idx)
                if not neighbor_row:
                    continue  # 节点不存在，跳过

                # --- 规则校验 1: 单父节点校验（无论向上还是向下，都要检查）---
                # 每个节点加入链路前都要检查其父节点数量
                # 如果节点的候选父节点数量大于1，整条链路都删除
                neighbor_parents = self.index_to_parents.get(neighbor_idx, [])
                if len(neighbor_parents) > 1:
                    # 根据 discard_mode 决定是整条链路作废还是只跳过该节点
                    if self.discard_mode == 'chain':
                        return None  # 整条链路作废
                    else:
                        continue  # 只跳过这个节点

                # --- 规则校验 2: Global ID 一致性 ---
                nid_gid = neighbor_row.get('global_id')
                if pd.notna(nid_gid) and nid_gid != '':
                    if len(seen_gids) > 0 and nid_gid not in seen_gids:
                        return None  # GID 冲突（>1种），整条链路作废
                    seen_gids.add(nid_gid)

                # 标记并入队
                # 注意：父节点可以有多个子节点，所有子节点都会继续追踪（不需要检查子节点数量）
                seen_indices[neighbor_idx] = next_hop
                queue.append((neighbor_idx, next_hop))

        return component_nodes

    def find_all_bidirectional_chains(
            self,
            root_conditions: List[Tuple[str, str]] = None,
            max_depth: int = 100
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        全量双向追踪入口
        
        参数:
            root_conditions: 入口筛选条件 [(系统, 交易码), ...]，如果为 None 则使用所有节点
            max_depth: 允许的最大跳数 (hops)，-1 表示无限制
        
        返回:
            (chains_df, stats)
            - chains_df: 完整链路数据 DataFrame，包含 trace_id
            - stats: 统计信息字典
        """
        start_time = time.time()

        # 1. 确定入口节点
        if root_conditions:
            root_nodes_list = []
            root_conditions_map = {}  # index -> (系统名, 交易码)
            
            for root_system, root_msg_type in root_conditions:
                nodes = self.df[
                    (self.df['srcSysname'] == root_system + '-F5') &
                    (self.df['msgType'] == root_msg_type)
                ]
                
                # 记录每个节点的起始条件
                for node_idx in nodes['index'].values:
                    root_conditions_map[node_idx] = (root_system, root_msg_type)
                
                root_nodes_list.append(nodes)
            
            if root_nodes_list:
                entry_nodes = pd.concat(root_nodes_list, ignore_index=True)
                candidate_indices = entry_nodes['index'].unique()
            else:
                entry_nodes = pd.DataFrame()
                candidate_indices = []
        else:
            entry_nodes = self.df
            candidate_indices = self.df['index'].values
            root_conditions_map = {}

        print(f"\n待处理入口节点数: {len(candidate_indices)}")
        print(f"最大追踪跳数 (Max Depth): {max_depth if max_depth != -1 else '无限制'}")

        if len(candidate_indices) == 0:
            print("警告：没有找到符合条件的起始节点")
            return pd.DataFrame(), {}

        global_visited = set()
        all_chain_dfs = []
        stats = {
            '起始节点数量': len(candidate_indices),
            '有效链路数量': 0,
            '被抛弃的路径': 0,
            '过滤的链路（global_id>1种）': 0,
            '总路径数': 0,
            '抛弃率': '0%',
            '抛弃模式': '整条链路' if self.discard_mode == 'chain' else '分支路径',
            '耗时': ''
        }

        trace_id_counter = 0

        n_roots = len(candidate_indices)
        print_interval = max(1, n_roots // 10)  # 每10%显示一次

        for idx, start_idx in enumerate(candidate_indices):
            if (idx + 1) % print_interval == 0 or (idx + 1) == n_roots:
                print(f"  进度: {idx + 1}/{n_roots} ({(idx + 1)*100//n_roots}%)")
            
            if start_idx in global_visited:
                continue

            # 2. 执行双向追踪
            chain_indices = self.trace_bidirectional(start_idx, max_depth=max_depth)

            if chain_indices:
                # 获取该节点的起始条件
                root_condition = root_conditions_map.get(start_idx, ('UNKNOWN', 'UNKNOWN'))
                
                rows = []
                for chain_idx in chain_indices:
                    row_data = self.index_to_row[chain_idx].copy()
                    row_data['trace_id'] = trace_id_counter
                    if root_condition:
                        row_data['root_system'] = root_condition[0]
                        row_data['root_msgType'] = root_condition[1]
                    rows.append(row_data)

                all_chain_dfs.append(pd.DataFrame(rows))

                global_visited.update(chain_indices)
                trace_id_counter += 1
                stats['有效链路数量'] += 1
            else:
                stats['被抛弃的路径'] += 1

        # 合并所有链路 DataFrame
        if all_chain_dfs:
            all_chains_df = pd.concat(all_chain_dfs, ignore_index=True)
            n_unique_chains_before = all_chains_df['trace_id'].nunique()
            
            # 检查每个 trace_id 的 global_id 情况（双重检查，虽然 trace_bidirectional 已经检查过）
            valid_trace_ids = []
            for trace_id in all_chains_df['trace_id'].unique():
                trace_data = all_chains_df[all_chains_df['trace_id'] == trace_id]
                global_ids = trace_data['global_id'].dropna().unique()
                
                if len(global_ids) <= 1:
                    valid_trace_ids.append(trace_id)
            
            # 过滤掉 global_id 超过一种的链路
            if valid_trace_ids:
                all_chains_df = all_chains_df[all_chains_df['trace_id'].isin(valid_trace_ids)]
                n_unique_chains = len(valid_trace_ids)
            else:
                all_chains_df = pd.DataFrame()
                n_unique_chains = 0
            
            filtered_count = n_unique_chains_before - n_unique_chains
            stats['过滤的链路（global_id>1种）'] = filtered_count
        else:
            all_chains_df = pd.DataFrame()
            n_unique_chains = 0
            filtered_count = 0

        elapsed = time.time() - start_time
        total_paths = n_unique_chains + stats['被抛弃的路径'] + filtered_count
        stats['总路径数'] = total_paths
        stats['抛弃率'] = f"{(stats['被抛弃的路径'] + filtered_count)/total_paths*100:.2f}%" if total_paths > 0 else "0%"
        stats['耗时'] = f"{elapsed:.2f}秒"

        print(f"\n双向追踪完成（抛弃模式: {stats['抛弃模式']}）：")
        print(f"  - 有效链路: {stats['有效链路数量']}")
        print(f"  - 被抛弃: {stats['被抛弃的路径']}")
        print(f"  - 过滤（global_id>1种）: {stats['过滤的链路（global_id>1种）']}")
        print(f"  - 抛弃率: {stats['抛弃率']}")
        print(f"  - 耗时: {stats['耗时']}")

        return all_chains_df, stats
    
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
            - start_nodes: 起始节点列表 [node_name, ...]
        """
        # 筛选指定 trace_id 的数据
        chain_data = chains_df[chains_df['trace_id'] == trace_id]
        
        if chain_data.empty:
            return {'nodes': [], 'edges': [], 'start_nodes': []}
        
        nodes = set()
        edges_set = set()  # 使用集合去重
        start_nodes = set()  # 起始节点集合
        
        # 识别起始节点：depth=1 或 depth=0（双向追踪时）
        # 如果没有 depth 字段，则根据 root_system 和 root_msgType 判断
        has_depth = 'depth' in chain_data.columns
        has_root_info = 'root_system' in chain_data.columns and 'root_msgType' in chain_data.columns
        
        if has_depth:
            # 找到最小 depth 的节点作为起始节点
            min_depth = chain_data['depth'].min()
            start_rows = chain_data[chain_data['depth'] == min_depth]
        elif has_root_info:
            # 根据 root_system 和 root_msgType 判断起始节点
            # 起始节点应该是 srcSysname == root_system + '-F5' 且 msgType == root_msgType
            root_system = chain_data.iloc[0]['root_system']
            root_msgType = chain_data.iloc[0]['root_msgType']
            if pd.notna(root_system) and pd.notna(root_msgType):
                start_rows = chain_data[
                    (chain_data['srcSysname'] == str(root_system) + '-F5') &
                    (chain_data['msgType'] == str(root_msgType))
                ]
            else:
                start_rows = chain_data.iloc[:1]  # 如果没有 root 信息，使用第一行
        else:
            # 默认使用第一行作为起始节点
            start_rows = chain_data.iloc[:1]
        
        # 遍历每行数据，提取节点和边
        for i, row in chain_data.iterrows():
            # 转换为字符串，处理 NaN 值
            src = str(row['srcSysname']) if pd.notna(row['srcSysname']) else 'None'
            dst = str(row['dstSysname']) if pd.notna(row['dstSysname']) else 'None'
            msg_type = str(row['msgType']) if pd.notna(row['msgType']) else 'None'
            
            # 添加节点
            nodes.add(src)
            nodes.add(dst)
            
            # 判断是否为起始节点（起始节点的源系统）
            if not start_rows.empty and i in start_rows.index:
                start_nodes.add(src)
            
            # 添加边（使用元组去重）
            edges_set.add((src, dst, msg_type))
        
        # 转换为列表格式
        edges = [
            {'source': src, 'target': dst, 'msgType': msg}
            for src, dst, msg in edges_set
        ]
        
        return {
            'nodes': sorted(list([str(i) for i in nodes])),  # 排序后返回列表
            'edges': edges,
            'start_nodes': sorted(list([str(i) for i in start_nodes]))  # 起始节点列表
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
            
            # 对边排序（按 source, target, msgType），确保所有值都是字符串
            edges_sorted = sorted(
                graph['edges'], 
                key=lambda x: (
                    str(x['source']) if pd.notna(x['source']) else 'None',
                    str(x['target']) if pd.notna(x['target']) else 'None',
                    str(x['msgType']) if pd.notna(x['msgType']) else 'None'
                )
            )
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
            
            # 诊断输出：多个起始节点对应相同图结构
            if len(root_conditions_list) > 1:
                print(f"[graph_structures] 多个入口节点有相同链路: graph_id={graph_id}, trace_ids={list(trace_ids)}, root_conditions={root_conditions_list}")
            
            results.append({
                'graph_id': graph_id,  # 添加图结构编号
                '图结构': signature,
                '链路数量': len(trace_ids),
                '节点数': len(example_graph['nodes']),
                '边数': len(example_graph['edges']),
                'trace_id': [int(tid) for tid in trace_ids],  # 转换为普通 Python int 列表
                'root_conditions': root_conditions_list,  # 所有起始条件
                '节点': example_graph['nodes'],
                '边': example_graph['edges'],
                '起始节点': example_graph.get('start_nodes', [])  # 起始节点列表
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
    discard_mode: str = 'chain'
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
    # chains_df, stats = tracer.find_chains_from_root(
    #     root_conditions=root_conditions,
    #     max_depth=max_depth
    # )
    chains_df, stats = tracer.find_all_bidirectional_chains(root_conditions, max_depth)
    
    if chains_df.empty:
        print("\n没有找到有效链路")
        return pd.DataFrame(), stats, pd.DataFrame()
    
    # 统计图结构
    graph_stats_df = tracer.count_graph_structures(chains_df)
    
    # 检查：除起始点外，候选父节点_gid_esb过滤 的值的长度是否有大于1的情况
    # 由于串联时使用的是候选父节点_gid_esb过滤/候选子节点_gid_esb过滤，
    # 这里也改为基于该列做一致性检查。
    if '候选父节点_gid_esb过滤' in chains_df.columns and 'depth' in chains_df.columns:
        non_root_nodes = chains_df[chains_df['depth'] > 1]
        if not non_root_nodes.empty:
            multi_parent_count = non_root_nodes[
                non_root_nodes['候选父节点_gid_esb过滤'].apply(lambda x: len(x) > 1)
            ]
            if not multi_parent_count.empty:
                print(f"\n⚠️ 警告：发现 {len(multi_parent_count)} 个非起始节点有多个候选父节点（gid+ESB过滤后）")
                print("示例节点：")
                print(multi_parent_count[['index', 'trace_id', 'depth', 'srcSysname', 'dstSysname', 'msgType', '候选父节点_gid_esb过滤']].head(5))
            else:
                print("\n✓ 检查通过：所有非起始节点的候选父节点_gid_esb过滤数量 ≤ 1")
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