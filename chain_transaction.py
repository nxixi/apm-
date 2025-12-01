import heapq
from collections import defaultdict, deque
import numpy as np
import pandas as pd

link_start_field = 'r_src_ip'
link_end_field = 'r_dst_ip'


def read_df(p):
    return pd.read_csv(p)[[link_start_field,link_end_field,'start_at_ms','latency_msec']]



def group_indices(s):
    g = s.groupby(s).indices
    return {k: np.fromiter(v, dtype=np.int64) for k,v in g.items()}

def link_one_ip(parents, children, start_ms, end_ms, left,right):
    if parents.size == 0 or children.size == 0: 
        return []
    p_start_adj = start_ms[parents] - left
    p_end_adj = end_ms[parents] + right
    order_p = np.argsort(p_start_adj, kind='mergesort')
    P = parents[order_p]
    P_start = p_start_adj[order_p]
    P_end = p_end_adj[order_p]
    C = children[np.argsort(start_ms[children], kind="mergesort")]
    heap = []
    span = (end_ms[P] - start_ms[P]).astype(np.int64)
    links = []
    i = 0
    for cr in C:
        cs, ce = int(start_ms[cr]), int(end_ms[cr])
        while i < P.size and P_start[i] <= cs:
            heapq.heappush(heap, (int(P_end[i]), int(span[i]), int(P[i])))
            i += 1
        while heap and heap[0][0] < ce: 
            heapq.heappop(heap)
        if heap:
            links.append((int(cr), heap[0][2]))
    return links

def link_all(df, left, right):
    n = df.shape[0]
    parent = np.full(n ,-1, dtype=np.int64)
    start_ms = df['start_at_ms'].to_numpy(np.int64, copy=False)
    end_ms = (df['start_at_ms'] + df["latency_msec"]).to_numpy(np.int64)
    s2p = group_indices(df[link_end_field])
    c2c = group_indices(df[link_start_field])
    for ip in set(s2p.keys()).intersection(c2c.keys()):
        for ch, pr in link_one_ip(s2p[ip], c2c[ip], start_ms, end_ms, left, right):
            parent[ch] = pr
    return parent


def trace_end_depth(parent):
    n = parent.shape[0]
    trace = np.full(n, -1, dtype=np.int64)
    depth = np.full(n, -1, dtype=np.int64)
    kids = defaultdict(list)
    for ch, pr in enumerate(parent):
        if pr >= 0:
            kids[int(pr)].append(int(ch))
    roots = np.where(parent < 0)[0]
    for r in roots:
        if trace[r] != -1:
            continue
        trace[r] = int(r)
        depth[r] = 0
        q = deque([int(r)])
        while q:
            cur = q.popleft()
            for k in kids.get(cur, ()):
                trace[k] = trace[cur]
                depth[k] = depth[cur] + 1
                q.append(k)
    mask = trace == -1
    if np.any(mask):
        idx = np.where(mask)[0]
        trace[idx] = idx
        depth[idx] = 0
    return trace, depth

def chain_df(df, left_ms = 100, right_ms = 100):
    parent = link_all(df, left_ms, right_ms)
    trace, depth = trace_end_depth(parent)
    out = df.copy()
    out["parent_row_id"] = parent
    out["trace_id"] = trace
    out["depth"] = depth
    return out