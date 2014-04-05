SELECT
  nation.name,
  sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue 
FROM
  customer, orders, nation, lineitem
WHERE
  lineitem.orderkey = orders.orderkey
  and nation.nationkey =  customer.nationkey
  and orders.custkey = customer.custkey
  and orders.orderdate >= DATE( '1994-01-01')
  and orders.orderdate < DATE ('1995-01-1')
GROUP BY nation.name
ORDER BY revenue desc;