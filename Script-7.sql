-- Выведите название самолетов, которые имеют менее 50 посадочных мест?

SELECT model, t.seats AS "seats" FROM (SELECT model, count(seat_no) AS "seats" FROM aircrafts a JOIN seats s ON a.aircraft_code = s.aircraft_code GROUP BY model) t
WHERE "seats" < 50 

--Выведите процентное изменение ежемесячной суммы бронирования билетов, округленной до сотых.


SELECT *, round((amount - LAG(amount) OVER(ORDER BY date_)) / LAG(amount) OVER(ORDER BY date_) * 100, 2) FROM (
	SELECT sum(total_amount) AS amount, date_trunc('month', book_date) AS date_ FROM bookings b 
	GROUP BY date_trunc('month', book_date)) t


--Выведите названия самолетов не имеющих бизнес - класс. Решение должно быть через функцию array_agg.
SELECT * FROM (SELECT aircraft_code, array_agg(fare_conditions) AS fare
	FROM (SELECT DISTINCT aircraft_code, fare_conditions FROM seats s ORDER BY aircraft_code, fare_conditions) f
	GROUP BY aircraft_code) t
WHERE fare[1] != 'Business' 

--Найдите процентное соотношение перелётов по маршрутам от общего количества перелётов. Выведите в результат названия аэропортов и процентное отношение.
--Используйте в решении оконную функцию.

SELECT SUM(percentage) FROM (SELECT departure_airport, round((flight_count * 100.0 / total_flights), 2) AS percentage FROM
  ( SELECT departure_airport, COUNT(*) AS flight_count, SUM(COUNT(*)) OVER () AS total_flights FROM flights
    GROUP BY departure_airport ) AS subquery) t
  
 --Классифицируйте финансовые обороты (сумму стоимости билетов) по маршрутам:
--до 50 млн – low
--от 50 млн включительно до 150 млн – middle
--от 150 млн включительно – high
--Выведите в результат количество маршрутов в каждом полученном классе.

SELECT CASE
    WHEN sum_amount < 50000000 THEN 'low'
    WHEN sum_amount >= 50000000 AND sum_amount < 150000000 THEN 'middle'
    ELSE 'high'
  END AS classification,
  COUNT(*) AS route_count
  FROM 
    (SELECT departure_airport, SUM(amount) AS sum_amount
	FROM (SELECT * FROM flights f
		 JOIN ticket_flights tf ON f.flight_id = tf.flight_id) t
		 GROUP BY departure_airport) AS subquery
GROUP BY classification;

  --Вычислите медиану стоимости билетов, медиану стоимости бронирования и отношение медианы бронирования к медиане стоимости билетов, результат округлите до сотых. 
WITH ticket_median AS (
  SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_ticket_amount
  FROM ticket_flights
),
booking_median AS (
  SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS median_booking_amount
  FROM bookings
)
SELECT
  ROUND(median_ticket_amount::numeric, 2) AS median_ticket_amount,
  ROUND(median_booking_amount::numeric, 2) AS median_booking_amount,
  ROUND((median_booking_amount / median_ticket_amount)::numeric, 2) AS booking_to_ticket_ratio
FROM
  ticket_median, booking_median;
 
 
 --Выведите накопительный итог количества мест в самолётах по каждому аэропорту на каждый день. Учтите только те самолеты, которые летали пустыми и только те дни, когда из одного аэропорта вылетело более одного такого самолёта. 
 --Выведите в результат код аэропорта, дату вылета, количество пустых мест и накопительный итог.

WITH empty_seats_count AS (
SELECT
departure_airport,
scheduled_departure,
MAX(seats - occupied__seats) AS empty_seats
FROM 
(
 WITH 
occupied_seats AS 
(SELECT count(ticket_no) AS occupied__seats, flight_id FROM ticket_flights GROUP BY flight_id),
seats AS 
(SELECT a.aircraft_code, count(seat_no) AS "seats" FROM aircrafts a JOIN seats s ON a.aircraft_code = s.aircraft_code GROUP BY a.aircraft_code) 
SELECT f.flight_id, scheduled_departure, departure_airport, f.aircraft_code, seats, COALESCE(occupied__seats, 0) as occupied__seats FROM flights f 
FULL JOIN seats ON f.aircraft_code = seats.aircraft_code
FULL JOIN occupied_seats ON f.flight_id = occupied_seats.flight_id
) t
GROUP BY departure_airport, scheduled_departure)
SELECT
 departure_airport,
 scheduled_departure,
 empty_seats,
  SUM(empty_seats) OVER (
    PARTITION BY departure_airport
    ORDER BY scheduled_departure
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_empty_seats
FROM
  empty_seats_count
WHERE
  empty_seats > 0
GROUP BY
  departure_airport,
  scheduled_departure,
  empty_seats
HAVING
  COUNT(*) > 1
ORDER BY
  departure_airport,
  scheduled_departure
  
  -- Выведите количество пассажиров по каждому коду сотового оператора. Код оператора – это три символа после +7
  
  
SELECT SUBSTRING((contact_data ->> 'phone') from 3 for 3) AS Код_оператора, COUNT(*) AS Количество_пассажиров
FROM tickets t 
WHERE contact_data ->> 'phone' LIKE '+7%'
GROUP BY SUBSTRING((contact_data ->> 'phone') from 3 for 3);
 
