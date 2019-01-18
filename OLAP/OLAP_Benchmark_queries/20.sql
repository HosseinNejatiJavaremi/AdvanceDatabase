-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
:x
:o
select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like ':1%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date ':2'
					and l_shipdate < date ':2' + interval '1' year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = ':3'
order by
	s_name;
:n -1


EXPLAIN ANALYZE
select	s_name,
	s_address
from	supplier,
	nation
where	s_suppkey in (
		select	ps_suppkey
		from	partsupp
		where	ps_partkey in (
				select	p_partkey
				from	part
				where	p_name like 'blue%')
			and ps_availqty > (
				select	0.5 * sum(l_quantity)
				from	lineitem
				where	l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1997-01-01'
					and l_shipdate < date '1997-01-01' + interval '1' year))
	and s_nationkey = n_nationkey
	and n_name = 'IRAN'
order by s_name;

