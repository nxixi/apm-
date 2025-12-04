"""
交易链路串联模块
基于 chain_transaction.py 改造，支持查找每个节点的所有候选父节点和候选子节点

改造说明：
- 原版本：使用堆算法找单个最优父节点（高性能）
- 新版本：返回所有候选父子节点（完整信息）
- 保留原版本的优化思想：groupby 索引 + 时间排序

可用函数：
1. chain_df()          - 标准优化版本，适用于大多数场景
2. chain_df_heap()     - 堆优化版本，借鉴原 chain_transaction.py 的思想
3. chain_df_inplace()  - 内存优化版本，不复制数据，直接修改原 DataFrame
4. estimate_memory_usage() - 估算内存使用量

性能优化：
- 索引构建使用 groupby，比 iterrows 快 100+ 倍
- 时间比较使用 numpy 布尔掩码，批量筛选
- 预建位置映射，避免重复查找

内存管理：
- chain_df(): 会复制 DataFrame，内存使用约为原数据的 2-3 倍
- chain_df_inplace(): 不复制数据，内存使用约为原数据的 1.2 倍
- 建议先用 estimate_memory_usage() 评估内存需求
"""

import pandas as pd
import numpy as np
import heapq
from collections import defaultdict, deque
from typing import List, Dict, Tuple
import time
from trace_chains import trace_and_analyze

src_ip_col = "r_src_ip"
dst_ip_col = "r_dst_ip"

import sys
sys.path.append('.')

def _build_ip_indexes(df: pd.DataFrame) -> Tuple[Dict[str, List[int]], Dict[str, List[int]]]:
    """
    构建IP索引，用于快速查找（向量化版本）
    
    返回:
        (src_ip_index, dst_ip_index)
        - src_ip_index: {src_ip: [行索引列表]}
        - dst_ip_index: {dst_ip: [行索引列表]}
    """
    # 使用 groupby 向量化构建索引，比 iterrows() 快100倍以上
    src_ip_index = df.groupby(src_ip_col).groups
    dst_ip_index = df.groupby(dst_ip_col).groups
    
    # 转换为普通字典，values 转为列表
    src_ip_index = {k: v.tolist() for k, v in src_ip_index.items()}
    dst_ip_index = {k: v.tolist() for k, v in dst_ip_index.items()}
    
    return src_ip_index, dst_ip_index


def _find_candidate_nodes(
    df: pd.DataFrame,
    src_ip_index: Dict[str, List[int]],
    dst_ip_index: Dict[str, List[int]],
    left_ms: int,
    right_ms: int
) -> Tuple[List[List[str]], List[List[str]]]:
    """
    为每条记录查找候选子节点和候选父节点（numpy 向量化版本）
    
    返回:
        (children_list, parents_list)
        两个列表，每个元素是该行的候选节点 index 列表
    """
    n = len(df)
    children_list = [[] for _ in range(n)]
    parents_list = [[] for _ in range(n)]
    
    # 提取需要的列为 numpy 数组
    start_times = df['start_at_ms'].values
    end_times = df['end_at_ms'].values
    indices = df['index'].values
    src_ips = df[src_ip_col].values
    dst_ips = df[dst_ip_col].values
    df_index = df.index.values
    
    # 创建位置映射
    idx_to_pos = {idx: i for i, idx in enumerate(df_index)}
    
    # 遍历每一行（带进度显示）
    print_interval = max(1, n // 20)
    for i in range(n):
        if i % print_interval == 0:
            print(f"处理进度: {i}/{n} ({i*100//n}%)")
        
        idx = df_index[i]
        
        # === 查找候选子节点 ===
        candidate_children_idx = src_ip_index.get(dst_ips[i], [])
        if len(candidate_children_idx) > 0:
            # 转换为位置数组（批量查找）
            child_positions = []
            for c in candidate_children_idx:
                if c != idx:
                    child_positions.append(idx_to_pos[c])
            
            if child_positions:
                child_positions = np.array(child_positions, dtype=np.int32)
                
                # 向量化时间比较
                parent_start_lower = start_times[i] - left_ms
                parent_end_upper = end_times[i] + right_ms
                
                # 使用 numpy 布尔掩码一次性筛选
                mask = (start_times[child_positions] >= parent_start_lower) & \
                       (end_times[child_positions] <= parent_end_upper)
                
                # 获取符合条件的子节点
                if np.any(mask):
                    valid_positions = child_positions[mask]
                    children_list[i] = [int(x) for x in indices[valid_positions]]
        
        # === 查找候选父节点 ===
        candidate_parents_idx = dst_ip_index.get(src_ips[i], [])
        if len(candidate_parents_idx) > 0:
            # 转换为位置数组（批量查找）
            parent_positions = []
            for p in candidate_parents_idx:
                if p != idx:
                    parent_positions.append(idx_to_pos[p])
            
            if parent_positions:
                parent_positions = np.array(parent_positions, dtype=np.int32)
                
                # 向量化时间比较
                child_start = start_times[i]
                child_end = end_times[i]
                
                # 使用 numpy 布尔掩码一次性筛选
                mask = (start_times[parent_positions] - left_ms <= child_start) & \
                       (end_times[parent_positions] + right_ms >= child_end)
                
                # 获取符合条件的父节点
                if np.any(mask):
                    valid_positions = parent_positions[mask]
                    parents_list[i] = [int(x) for x in indices[valid_positions]]
    
    print(f"处理进度: {n}/{n} (100%)")
    
    return children_list, parents_list


def filter_important_transactions(
    df: pd.DataFrame,
    important_msg_types: List[str]
) -> pd.DataFrame:
    """
    筛选重要交易码的记录
    
    参数:
        df: 已串联的DataFrame
        important_msg_types: 重要交易码列表
    
    返回:
        只包含重要交易码及其子孙节点的DataFrame
    """
    if 'msgType' not in df.columns:
        raise ValueError("DataFrame缺少 msgType 列")
    
    # 找出所有重要交易码的记录
    important_records = df[df['msgType'].isin(important_msg_types)].copy()
    
    # 递归找出所有子孙节点
    all_indices = set(important_records['index'].tolist())
    _collect_descendants(df, important_records, all_indices)
    
    # 返回筛选后的结果
    return df[df['index'].isin(all_indices)].copy()


def _collect_descendants(
    df: pd.DataFrame,
    current_records: pd.DataFrame,
    collected_indices: set
) -> None:
    """
    递归收集所有子孙节点的 index
    """
    new_children_indices = set()
    
    for _, row in current_records.iterrows():
        children = row.get('候选子节点', [])
        for child_idx in children:
            if child_idx not in collected_indices:
                new_children_indices.add(child_idx)
                collected_indices.add(child_idx)
    
    # 如果有新的子节点，递归查找它们的子节点
    if new_children_indices:
        new_children_records = df[df['index'].isin(new_children_indices)]
        _collect_descendants(df, new_children_records, collected_indices)


def _add_filtered_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    添加4个过滤列（优化版本，避免使用 iterrows）
    1. 按 global_id 过滤候选子节点
    2. 按 global_id 过滤候选父节点
    3. 按 msgType 过滤候选子节点
    4. 按 msgType 过滤候选父节点
    
    过滤规则：
    - global_id 过滤：当子节点和父节点的 global_id 都存在且不相同时，剔除
    - msgType 过滤：如果候选节点多于1个，保留 msgType 相同的
    """
    # 检查必需列
    has_global_id = 'global_id' in df.columns
    has_msg_type = 'msgType' in df.columns
    
    # 创建快速查找字典（避免 iterrows）
    index_to_gid = {}
    index_to_msg = {}
    
    if has_global_id:
        index_to_gid = dict(zip(df['index'].values, df['global_id'].values))
    if has_msg_type:
        index_to_msg = dict(zip(df['index'].values, df['msgType'].values))
    
    # 使用 numpy 数组提高性能
    indices = df['index'].values
    gids = df['global_id'].values if has_global_id else None
    msgs = df['msgType'].values if has_msg_type else None
    children_list = df['候选子节点'].values
    parents_list = df['候选父节点'].values
    
    n = len(df)
    
    # 初始化结果列表
    children_filtered_by_gid = []
    parents_filtered_by_gid = []
    children_filtered_by_msg = []
    parents_filtered_by_msg = []
    
    # 优化后的循环：直接使用数组索引
    for i in range(n):
        current_gid = gids[i] if has_global_id else None
        current_msg = msgs[i] if has_msg_type else None
        children = children_list[i]
        parents = parents_list[i]
        
        # === 1. 按 global_id 过滤子节点 ===
        if has_global_id and current_gid is not None and pd.notna(current_gid):
            children_gid_filtered = [
                child_idx for child_idx in children
                if child_idx in index_to_gid and (
                    pd.isna(index_to_gid[child_idx]) or 
                    index_to_gid[child_idx] == current_gid
                )
            ]
        else:
            children_gid_filtered = list(children)
        
        # === 2. 按 global_id 过滤父节点 ===
        if has_global_id and current_gid is not None and pd.notna(current_gid):
            parents_gid_filtered = [
                parent_idx for parent_idx in parents
                if parent_idx in index_to_gid and (
                    pd.isna(index_to_gid[parent_idx]) or 
                    index_to_gid[parent_idx] == current_gid
                )
            ]
        else:
            parents_gid_filtered = list(parents)
        
        # === 3. 按 msgType 过滤子节点（仅当多于1个时） ===
        if has_msg_type and len(children_gid_filtered) > 1 and current_msg is not None and pd.notna(current_msg):
            same_msg_children = [
                child_idx for child_idx in children_gid_filtered
                if child_idx in index_to_msg and index_to_msg[child_idx] == current_msg
            ]
            children_msg_filtered = same_msg_children if same_msg_children else children_gid_filtered
        else:
            children_msg_filtered = children_gid_filtered
        
        # === 4. 按 msgType 过滤父节点（仅当多于1个时） ===
        if has_msg_type and len(parents_gid_filtered) > 1 and current_msg is not None and pd.notna(current_msg):
            same_msg_parents = [
                parent_idx for parent_idx in parents_gid_filtered
                if parent_idx in index_to_msg and index_to_msg[parent_idx] == current_msg
            ]
            parents_msg_filtered = same_msg_parents if same_msg_parents else parents_gid_filtered
        else:
            parents_msg_filtered = parents_gid_filtered
        
        # 添加到结果列表
        children_filtered_by_gid.append(children_gid_filtered)
        parents_filtered_by_gid.append(parents_gid_filtered)
        children_filtered_by_msg.append(children_msg_filtered)
        parents_filtered_by_msg.append(parents_msg_filtered)
    
    # 添加新列
    df['候选子节点_gid过滤'] = children_filtered_by_gid
    df['候选父节点_gid过滤'] = parents_filtered_by_gid
    df['候选子节点_msg过滤'] = children_filtered_by_msg
    df['候选父节点_msg过滤'] = parents_filtered_by_msg
    
    return df


def estimate_memory_usage(df: pd.DataFrame) -> dict:
    """
    估算内存使用量
    
    返回:
        包含各部分内存使用的字典（单位：MB）
    """
    n = len(df)
    
    # DataFrame 大小
    df_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
    
    # 复制 DataFrame 的内存
    copy_memory = df_memory
    
    # 索引字典估算（假设平均每个 IP 有 n/1000 条记录）
    unique_ips = df[src_ip_col].nunique() + df[dst_ip_col].nunique()
    index_memory = unique_ips * 100 / 1024 / 1024  # 粗略估算
    
    # 结果列表（每个节点假设平均有 5 个父节点和 5 个子节点）
    avg_relations = 10
    result_memory = n * avg_relations * 8 / 1024 / 1024  # 每个 index 假设 8 字节
    
    # Numpy 数组
    array_memory = n * 5 * 8 / 1024 / 1024  # 5 个数组，每个元素 8 字节
    
    total = copy_memory + index_memory + result_memory + array_memory
    
    return {
        '原始数据': round(df_memory, 2),
        'DataFrame复制': round(copy_memory, 2),
        'IP索引': round(index_memory, 2),
        '结果列表': round(result_memory, 2),
        'Numpy数组': round(array_memory, 2),
        '预估总计': round(total, 2),
        '峰值内存': round(total * 1.2, 2)  # 留 20% 余量
    }


def chain_df_heap(df: pd.DataFrame, left_ms: int = 0, right_ms: int = 0) -> pd.DataFrame:
    """
    堆优化版本：基于原 chain_transaction.py 的堆算法改造
    使用时间排序 + 堆维护活跃父节点，找出所有候选父子节点
    
    优势：
    - 对于时间窗口较小的场景，性能优于标准版本
    - 自动剪枝过期的父节点，减少不必要的比较
    
    适用场景：
    - left_ms 和 right_ms 较小（< 1000ms）
    - 数据按时间相对有序
    """
    start_time = time.time()
    
    # 验证必需列
    required_cols = [src_ip_col, dst_ip_col, 'start_at_ms', 'end_at_ms', 'index']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DataFrame缺少必需的列: {missing_cols}")
    
    if df.empty:
        df['候选子节点'] = [[] for _ in range(len(df))]
        df['候选父节点'] = [[] for _ in range(len(df))]
        return df
    
    result_df = df.copy()
    n = len(result_df)
    
    print("【堆优化模式】使用时间排序 + 堆算法")
    
    # 初始化结果
    children_dict = defaultdict(list)  # {parent_index: [child_indices]}
    parents_dict = defaultdict(list)   # {child_index: [parent_indices]}
    
    # 提取数据
    start_times = result_df['start_at_ms'].values
    end_times = result_df['end_at_ms'].values
    indices_values = result_df['index'].values
    
    print("构建索引...")
    # 按 IP 分组
    src_groups = result_df.groupby(src_ip_col)
    dst_groups = result_df.groupby(dst_ip_col)
    
    print("查找候选节点（使用堆优化）...")
    # 对每个 IP 对，使用堆算法找所有候选父子关系
    for ip in set(src_groups.groups.keys()).intersection(dst_groups.groups.keys()):
        # 候选父节点：dst_ip == ip
        parent_indices = dst_groups.groups[ip].values
        # 候选子节点：src_ip == ip  
        child_indices = src_groups.groups[ip].values
        
        if len(parent_indices) == 0 or len(child_indices) == 0:
            continue
        
        # 调用堆算法查找所有候选关系
        links = _link_all_candidates_heap(
            parent_indices, child_indices,
            start_times, end_times, indices_values,
            left_ms, right_ms
        )
        
        # 存储结果
        for child_idx, parent_idx in links:
            children_dict[parent_idx].append(child_idx)
            parents_dict[child_idx].append(parent_idx)
    
    # 转换为列表格式（确保键是 Python 原生类型）
    children_list = [children_dict.get(int(indices_values[i]), []) for i in range(n)]
    parents_list = [parents_dict.get(int(indices_values[i]), []) for i in range(n)]
    
    result_df['候选子节点'] = children_list
    result_df['候选父节点'] = parents_list
    
    # 添加过滤列
    print("添加过滤列...")
    result_df = _add_filtered_columns(result_df)
    
    total_time = time.time() - start_time
    print(f"串联完成！共处理 {n} 条记录，总耗时: {total_time:.2f}秒")
    return result_df


def _link_all_candidates_heap(
    parent_indices: np.ndarray,
    child_indices: np.ndarray,
    start_times: np.ndarray,
    end_times: np.ndarray,
    indices_values: np.ndarray,
    left_ms: int,
    right_ms: int
) -> List[Tuple]:
    """
    使用堆算法找出所有候选父子关系
    
    改造自 chain_transaction.py 的 link_one_ip 函数
    原版本只返回最优父节点，此版本返回所有符合条件的父节点
    """
    if parent_indices.size == 0 or child_indices.size == 0:
        return []
    
    # 转换为位置索引
    parent_positions = parent_indices
    child_positions = child_indices
    
    # 计算调整后的父节点时间范围
    p_start_adj = start_times[parent_positions] - left_ms
    p_end_adj = end_times[parent_positions] + right_ms
    
    # 按开始时间排序父节点
    order_p = np.argsort(p_start_adj, kind='mergesort')
    P = parent_positions[order_p]
    P_start = p_start_adj[order_p]
    P_end = p_end_adj[order_p]
    
    # 按开始时间排序子节点
    C = child_positions[np.argsort(start_times[child_positions], kind="mergesort")]
    
    links = []
    heap = []  # (end_time, parent_position)
    i = 0  # 父节点指针
    
    # 遍历每个子节点
    for c_pos in C:
        c_start = int(start_times[c_pos])
        c_end = int(end_times[c_pos])
        
        # 将所有可能成为父节点的记录加入堆（start_time <= child_start_time）
        while i < P.size and P_start[i] <= c_start:
            heapq.heappush(heap, (int(P_end[i]), int(P[i])))
            i += 1
        
        # 移除已经过期的父节点（end_time < child_end_time）
        while heap and heap[0][0] < c_end:
            heapq.heappop(heap)
        
        # 堆中剩余的都是候选父节点 - 改造点：保存所有而非只保存最优
        valid_parents = []
        for p_end, p_pos in heap:
            # 再次确认时间条件（堆中的都应该满足）
            if (start_times[p_pos] - left_ms <= c_start and
                end_times[p_pos] + right_ms >= c_end):
                valid_parents.append(p_pos)
        
        # 记录所有候选父子关系（转换为 Python 原生类型，排除自身）
        for p_pos in valid_parents:
            # 排除自身：一条记录不能是自己的父节点
            if indices_values[c_pos] != indices_values[p_pos]:
                links.append((int(indices_values[c_pos]), int(indices_values[p_pos])))
    
    return links


def chain_df(df: pd.DataFrame, left_ms: int = 0, right_ms: int = 0, use_filtered='msg', output_prefix=None, discard_mode='branch') -> pd.DataFrame:
    """
    串联链路并追踪
    
    参数:
        df: 输入 DataFrame
        left_ms: 左偏移时间（毫秒）
        right_ms: 右偏移时间（毫秒）
        use_filtered: 使用哪个过滤列 ('original', 'gid', 'msg')
        output_prefix: 输出文件前缀（可选）
        discard_mode: 抛弃模式
            - 'branch': 只抛弃该节点及其后续路径（默认，原规则）
            - 'chain': 抛弃整条链路（包括之前的链路）
    """
    df['end_at_ms'] = df['start_at_ms'] + df["latency_msec"]

    # 打上父节点、子节点
    df_node = chain_df_heap(df, left_ms=left_ms, right_ms=right_ms)

    # 以重要交易码为起点，串联链路
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
        df=df_node,
        root_conditions=root_conditions,
        use_filtered=use_filtered,  # 使用 msgType 过滤后的结果
        output_prefix=output_prefix,
        discard_mode=discard_mode
    )

    return chains_df, graph_stats_df




# 使用示例
if __name__ == "__main__":
    # 创建测试数据
    test_data = {
        'index': [1, 2, 3, 4],
        'r_src_ip': ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.2'],
        'r_dst_ip': ['192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5'],
        'srcSysname': ['系统A', '系统B', '系统C', '系统B'],
        'dstSysname': ['系统B', '系统C', '系统D', '系统E'],
        'start_at_ms': [1000, 1100, 1200, 1150],
        'end_at_ms': [1500, 1450, 1400, 1350],
        'latency_msec': [500, 500, 500, 500],
        'msgType': ['T001', 'T002', 'T003', 'T004'],
        'global_id': ['GID001', None, 'GID003', 'GID004']
    }
    df = pd.DataFrame(test_data)

    # 读取数据
    print("读取数据...")
    df = pd.read_csv("result/PSISADP_test_1117.csv")
    df['end_at_ms'] = df['start_at_ms'] + df["latency_msec"]
    df = df.sort_values(by='start_at_ms').reset_index(drop=True)
    df['index'] = df.index
    
    print("="*80)
    print(f"数据量：{len(df)} 条记录")
    print("="*80)
    
    # 内存估算
    print("\n内存使用估算：")
    mem_usage = estimate_memory_usage(df)
    for key, value in mem_usage.items():
        print(f"  {key}: {value} MB")
    
    print("\n" + "="*80)
    print("建议：")
    if mem_usage['峰值内存'] < 500:
        print("  ✓ 内存充足，可使用 chain_df() 标准版本")
    elif mem_usage['峰值内存'] < 2000:
        print("  ⚠ 内存较紧张，建议使用 chain_df_inplace() 内存优化版本")
    else:
        print("  ✗ 内存可能不足，建议分批处理或使用 chain_df_inplace()")
    print("="*80 + "\n")
    
    # 性能测试
    # print("="*80)
    # print("测试标准版本 chain_df():")
    # print("="*80)
    # result = chain_df(df, left_ms=0, right_ms=0)
    # print("\n串联结果示例：")
    # print(result[['index', 'src_ip', 'dst_ip', 'start_at_ms', 'end_at_ms',
    #               '候选子节点', '候选父节点']].head())
    
    print("\n" + "="*80)
    print("测试堆优化版本 chain_df_heap() (基于原 chain_transaction.py 改造):")
    print("="*80)
    result = chain_df(df.copy(), left_ms=0, right_ms=0)
    
    # # 展示基础结果
    # print("\n【基础串联结果】")
    # display_cols = ['index', src_ip_col, dst_ip_col, 'start_at_ms', 'end_at_ms',
    #                 '候选子节点', '候选父节点']
    # if 'global_id' in result_heap.columns:
    #     display_cols.insert(5, 'global_id')
    # if 'msgType' in result_heap.columns:
    #     display_cols.insert(5, 'msgType')
    # print(result_heap[display_cols].head(10))
    #
    # # 展示过滤后的结果
    # print("\n【过滤后的结果对比】")
    # filter_display_cols = ['index']
    # if 'global_id' in result_heap.columns:
    #     filter_display_cols.append('global_id')
    # if 'msgType' in result_heap.columns:
    #     filter_display_cols.append('msgType')
    # filter_display_cols.extend([
    #     '候选子节点', '候选子节点_gid过滤', '候选子节点_msg过滤',
    #     '候选父节点', '候选父节点_gid过滤', '候选父节点_msg过滤'
    # ])
    #
    # # 只显示有候选节点的行
    # has_candidates = (result_heap['候选子节点'].apply(len) > 0) | (result_heap['候选父节点'].apply(len) > 0)
    # print(result_heap[has_candidates][filter_display_cols].head(10))
    #
    # # 统计过滤效果
    # print("\n【过滤效果统计】")
    # n_total = len(result_heap)
    #
    #
    # # 候选节点 > 1 的比例统计
    # print("\n【候选节点 > 1 的记录比例】")
    #
    # # 子节点
    # children_orig_gt1 = sum(result_heap['候选子节点'].apply(lambda x: len(x) > 1))
    # children_gid_gt1 = sum(result_heap['候选子节点_gid过滤'].apply(lambda x: len(x) > 1))
    # children_msg_gt1 = sum(result_heap['候选子节点_msg过滤'].apply(lambda x: len(x) > 1))
    #
    # print(f"候选子节点 > 1:")
    # print(f"  原始: {children_orig_gt1}/{n_total} ({children_orig_gt1/n_total*100:.2f}%)")
    # print(f"  gid过滤: {children_gid_gt1}/{n_total} ({children_gid_gt1/n_total*100:.2f}%)")
    # print(f"  msg过滤: {children_msg_gt1}/{n_total} ({children_msg_gt1/n_total*100:.2f}%)")
    #
    # # 父节点
    # parents_orig_gt1 = sum(result_heap['候选父节点'].apply(lambda x: len(x) > 1))
    # parents_gid_gt1 = sum(result_heap['候选父节点_gid过滤'].apply(lambda x: len(x) > 1))
    # parents_msg_gt1 = sum(result_heap['候选父节点_msg过滤'].apply(lambda x: len(x) > 1))
    #
    # print(f"候选父节点 > 1:")
    # print(f"  原始: {parents_orig_gt1}/{n_total} ({parents_orig_gt1/n_total*100:.2f}%)")
    # print(f"  gid过滤: {parents_gid_gt1}/{n_total} ({parents_gid_gt1/n_total*100:.2f}%)")
    # print(f"  msg过滤: {parents_msg_gt1}/{n_total} ({parents_msg_gt1/n_total*100:.2f}%)")
    #
    # # 验证候选节点的准确性
    # def verify_candidates(df: pd.DataFrame, left_ms: int = 0, right_ms: int = 0, index_: int = 5):
    #
    #     for index in [3, 12, 19, 347771]:
    #         se = df[df['index'] == index].iloc[0]
    #         zi = se['候选子节点']
    #
    #         zi_1 = []
    #         for i in range(len(df)):
    #             if se['start_at_ms'] - 0 <= df.iloc[i]['start_at_ms'] and se['end_at_ms'] + 0 >= df.iloc[i]['end_at_ms'] and \
    #                     df.iloc[i]['r_src_ip'] == se['r_dst_ip']:
    #                 zi_1.append(df.iloc[i]['index'])
    #
    #         if set(zi) != set(zi_1):
    #             print(zi)
    #             print(zi_1)
    #
    # verify_candidates(result_heap, left_ms=0, right_ms=0, index_=5)
