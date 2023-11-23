# Техническое задание

Даны две таблицы (см ниже). Напишите SQL-запрос, чтобы найти сотрудников, которые получают три самые низкие зарплаты в каждом отделе.  

CREATE TABLE test2 AS
SELECT  1 AS id, 'Joel' AS name,  85000 AS Sel, 1  AS dep_id  
UNION  
SELECT 2, 'Henry', 80000, 2  
UNION  
SELECT 3,  'Samm',   60000,   2  
UNION  
SELECT 4, 'Maximus', 90000, 1  
UNION  
SELECT 5,  'Janet', 69000 , 1  
UNION  
SELECT  6, 'Randy', 85000, 1  
UNION  
SELECT 7,  'Will', 70000, 1
 

CREATE TABLE test3 AS  
SELECT 1 AS id,  'IT' AS dep_name  
UNION  
SELECT 2 AS id,  'Sales' 

# Решение

* Оконная функция с ранжированием строк при группировке в оконной функции по департаментам и сортировке по возрастанию зарплаты
* Наложение ограничения на значение выводимого номера строки