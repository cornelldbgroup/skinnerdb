select
    n_name as nation,
    extract(year from o_orderdate) as o_year,
    sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as amount
from
    part,
    supplier,
    lineitem,
    partsupp,
    orders,
    nation
where
        s_suppkey = l_suppkey
  and ps_suppkey = l_suppkey
  and ps_partkey = l_partkey
  and p_partkey = l_partkey
  and o_orderkey = l_orderkey
  and s_nationkey = n_nationkey
  and p_name like '%yellow%'

group by
    nation,
    o_year
order by
    nation,
    o_year desc;

