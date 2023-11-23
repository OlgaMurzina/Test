with
/* создали временную таблицу с ранжированием в окнах по dep_name и сортировкой по sel */
table_range as
(select t2.name,
	   t2.sel,
       row_number()  over (partition by t3.dep_name order by t2.sel) rang,
       t3.dep_name
from test2 t2 left join test3 t3 on t2.dep_id=t3.id)

/* основной запрос с ограничением на rang */
select * 
from table_range
where rang < 4
