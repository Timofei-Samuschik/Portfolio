-- Шаг 1. Импорт данных.
-- Создание базы данных "kion_data"
CREATE DATABASE IF NOT EXISTS kion_data;

-- Использование базы данных "kion_data"
USE kion_data;

-- Создание таблицы "users" для хранения данных о пользователях
CREATE TABLE users
(
 user_id INT,
 age VARCHAR(255),
 income VARCHAR(255),
 sex VARCHAR(255),
 kids_flg INT
 );	

-- Выборка всех данных из таблицы "users"
SELECT * 
FROM users;
 
-- Импорт данных из файла "users.csv" в таблицу "users" с разделителями ","
-- Пропуск первой строки файла
LOAD DATA INFILE 'users.csv' INTO TABLE users
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

-- Создание таблицы "items" для хранения данных о элементах контента
CREATE TABLE items
(
 item_id INT,
 content_type VARCHAR(255),
 title VARCHAR(255),
 title_orig VARCHAR(255),
 release_year VARCHAR(255),
 genres VARCHAR(500),
 countries VARCHAR(255),
 for_kids VARCHAR(255),
 age_rating VARCHAR(255),
 studios VARCHAR(255),
 directors VARCHAR(1000),
 actors VARCHAR(6500),
 descriptin VARCHAR(3000),
 keywords VARCHAR(3000)
 );

-- Выборка всех данных из таблицы "items"
SELECT * FROM items;

-- Импорт данных из файла "items.csv" в таблицу "items" с разделителями "," и заключенными в двойные кавычки полями
-- Пропуск первой строки файла
LOAD DATA INFILE 'items.csv' INTO TABLE items 
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
IGNORE 1 LINES;

-- Создание таблицы "interactions" для хранения данных о взаимодействиях пользователей с контентом
CREATE TABLE interactions
(
 user_id INT,
 item_id INT,
 last_watch_dt DATE,
 total_dur INT,
 watched_pct DOUBLE
 );	

-- Выборка всех данных из таблицы "interactions"
SELECT * FROM interactions;

-- Импорт данных из файла "interactions.csv" в таблицу "interactions" с разделителями "," и заключенными в двойные кавычки полями
-- Пропуск первой строки файла
-- Присвоение полю "watched_pct" значения NULL, если значение равно пустой строке
LOAD DATA INFILE 'interactions.csv'
INTO TABLE interactions 
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
IGNORE 1 LINES
(user_id, item_id, last_watch_dt, total_dur, @watched_pct)
SET watched_pct = NULLIF(@watched_pct, '');

-- Шаг 2. Создание продуктовй метрики "осознанно смотрящий пользователь"
-- Сначала просмотрим на данные каждой из таблиц, чтобы понять, на основании чего можно считать метрику осознанного смотрения.
-- Проанализировав имеющиеся данные, я пришел к следующему подходу расчета метрики осознанного смотрения: так как нет информации о паузах, перемотках, динамической истории просмотра (когда в один день фильм/сериал был просмотрен например на 10%, а на следующий день – на 20%) для расчета пригодятся данные о виде о продолжительности просмотра, проценте просмотра, id фильма/сериала, user id и тип контента. 
-- Здесь стоит остановиться и сказать, что на основе имеющихся данных под категорию осознанно смотрящих пользователей попадут и те пользователи, которые смотрят контент фоном, занимаясь, например, уборкой.
-- Соответственно алгоритм расчета осознанно смотрящего пользователя следующий:
-- 1. Для каждого пользователя оценить степень осознанного смотрения каждого фильма/сериала. Для этого введем соответствующий рейтинг в зависимости от типа контена:
	-- Для фильмов: 
		-- 1 для фильмов, у  которых общая продолжительность просмотра меньше 15 минут (15 минут происходит из предположения, что первые 15 минут фильма должны заинтересовать зрителя, и, если он перешел этот порог, то фильм его заинтересовал)
		-- 2 для фильмов, у которых общая продолжительность просмотра больше 15 минут 900 (Рейтинг 2 вводится для того чтобы отсеять фильмы, у которых продолжительность просмотра больше 15 минут, то есть фильм заинтересовал человека, и он осознанно смотрит его, однако допускается, что по каким то причинам, пользователь мог недосмотреть фильмы до конца, потому что он ему не понравился, и он не будет его досматривать или потому что что-то прервало просмотр. И так как мы не знаем причины остановки просмотра, но знаем процент, лучше выделить отдельную категорию для таких неоднозначных пользователей)
		-- 3 для фильмов, у которых общая продолжительность просмотра больше 15 минут и процент просмотра меньше 50% (Это пользователи, которые просмотрели 50%+ процентов фильма и скорее всего прервали просмотри по каким-то причинам, не связанным с интересами к фильму. Однако такие пользователи все еще не досмотрели фильм до конца, и мы не можем уверенно утверждать, что это не из-за интереса к фильму)
		-- 4 для фильмов, у которых общая продолжительность просмотра больше 15 минут и процент просмотра больше 90% (Это пользователи, которые досмотрели фильм до конца, 10% даются на предположение, что пользователь не стал смотреть титры)
	-- Для сериалов: 
		-- 1 для сериалов, у которых общая продолжительность просмотра меньше 30 минут (здесь такое же предположение, как и с фильмами, только во внимание берется тот факт, что 1 серия сериалов обычно больше остальных серий) 
		-- 2 для сериалов, у которых общая продолжительность просмотра больше 30 минут и процент просмотра меньше 50% (С сериалами после первой рейтинга 1 сложнее чем с фильмами, поскольку нельзя определить, забросил ли пользователь сериал или просто продолжает его смотреть из-за как уже было написано отсутствия динамичных данных просмотра)
		-- 3 для фильмов, у которых общая продолжительность просмотра больше 15 минут и процент просмотра меньше 50% (Здесь логика такая же, как и у фильмов)
		-- 4 для сериалов, у которых общая продолжительность просмотра больше 15 минут и процент просмотра больше 85%. (здесь логика такая же, как и у фильмов)
-- 2. Каждому пользователю присвоим рейтинг:
	-- Для фильмов: 1 если у пользователя количество просмотренных фильмов с рейтингом 3 и 4 больше количества просмотренного контента с рейтингом 1 и 2, и 0, если наоборот
	-- Для сериалов: 1 если у пользователя количество просмотренных сериалов с рейтингом 3 и 4 больше количества просмотренного контента с рейтингом 1 и 2, и 0, если наоборот
-- Для расчета данной метрики нам понадобятся таблица interactions и столбец content_type из items.  

-- Создание временной таблицы 'first_500t', которая содержит первые 500 000 записей из таблицы 'interactions'.
with first_500t as (
select * from interactions limit 500000
),
-- Создание временной таблицы 'data_merged', которая объединяет данные из 'first_500t' с информацией о контенте и вычисляет рейтинг осознанности 'consciousness_rating' для каждого фильма/сериала на основе правил сверху.
data_merged as (
select user_id, first_500t.item_id, last_watch_dt, total_dur, watched_pct, content_type,
CASE
	WHEN content_type = 'film' THEN
		CASE
			WHEN total_dur < 900 THEN 1
			WHEN total_dur > 900 and watched_pct < 50 THEN 2
			WHEN total_dur > 900 and watched_pct >= 50 and watched_pct < 85 THEN 3
			WHEN watched_pct >= 85 THEN 4
	END
	WHEN content_type = 'series' THEN
		CASE
			WHEN total_dur < 1800 THEN 1
			WHEN total_dur > 1800 and watched_pct < 50 THEN 2
			WHEN total_dur > 1800 and watched_pct >= 50 and watched_pct < 85 THEN 3
			WHEN watched_pct >= 85 THEN 4
	END
END AS consciousness_rating
from first_500t
left join (select item_id, content_type from items) as items_content_type on first_500t.item_id = items_content_type.item_id
order by user_id, last_watch_dt
),
-- Создание временной таблицы 'data_rated', которая вычисляет рйетинг осознанности пользователя 'user_consciousness_rating' на основе подсчета и суммирования значений 'consciousness_rating'.
data_rated as  (SELECT
    user_id, item_id, content_type, total_dur, watched_pct, consciousness_rating,
    CASE
		WHEN content_type = 'film' THEN
			CASE
				WHEN COUNT(consciousness_rating IN (3,4)) > SUM(consciousness_rating IN (1,2)) THEN 1
				ELSE 0
			END
		ELSE
			CASE
				WHEN content_type = 'series' THEN
					CASE
						WHEN COUNT(consciousness_rating IN (3,4)) > SUM(consciousness_rating in (1,2)) THEN 1
						ELSE 0
					END
			END
    END AS user_consciousness_rating
FROM data_merged
GROUP BY 1,2,3,4,5,6)

-- Выборка данных из 'data_rated' и группировка их по 'user_consciousness_rating' и 'content_type', с вычислением средних значений 'total_dur' и 'watched_pct'.
select user_consciousness_rating, content_type, avg(total_dur), avg(watched_pct)
from data_rated
group by 1,2 ;



