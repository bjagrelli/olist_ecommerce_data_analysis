-- QUANTIDADE DE PEDIDOS POR CIDADE
select upper(seller_city) as CIDADE, seller_state as UF, count(*) as QTD_PEDIDOS from
(select seller_city, seller_state, order_item_id, price, freight_value from olist.order_items oi
left join olist.sellers on sellers.seller_id = oi.seller_id) a
group by seller_city, seller_state order by 3 desc;

-- PEDIDOS QUE TINHAM A DATA LIMITE DE ENVIO E SÓ CHEGARAM NO CARRIER DEPOIS DISSO
select a.*, case when carrier_date > limit_date then 'ATRASO' else 'EM TEMPO' end from (
select orders.*,
to_timestamp(order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS') as carrier_date, 
to_timestamp(shipping_limit_date, 'YYYY-MM-DD HH24:MI:SS') as limit_date 
from olist.orders 
left join olist.order_items oit on oit.order_id = orders.order_id) a

