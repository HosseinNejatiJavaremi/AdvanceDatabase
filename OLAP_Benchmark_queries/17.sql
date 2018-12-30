-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = ':1'
	and p_container = ':2'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);
:n -1

BEGIN;
  EXPLAIN ANALYZE
  select
    sum(l_extendedprice) / 7.0 as avg_yearly
  from	lineitem,
    part
  where	p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'JUMBO JAR'
    and l_quantity < (
      select 0.2 * avg(l_quantity)
      from lineitem
      where l_partkey = p_partkey);
COMMIT;
