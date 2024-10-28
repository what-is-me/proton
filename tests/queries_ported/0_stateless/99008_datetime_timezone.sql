DROP STREAM IF EXISTS Dates;

SELECT to_datetime64('2022-12-30 13:44:17.000+08:00', 3, 'Europe/London'), to_datetime('2022-12-30 13:44:17+08:00', 'Europe/London');

CREATE stream Dates(t1 datetime('Europe/London'), t2 datetime, t3 datetime('Australia/Sydney')) ENGINE = Memory();

INSERT INTO Dates VALUES (to_datetime('2024-06-09 13:28:00','Europe/London'), to_time('6/9/2024 13:28','Europe/London'), to_time('6/9/2024 13:28','Europe/London'));

SELECT t1, t2, t3 FROM Dates;

DROP STREAM IF EXISTS Dates;

CREATE stream Dates(t1 datetime64(3, 'Europe/London'), t2 datetime64(3, 'America/Vancouver'), t3 datetime64(3)) ENGINE = Memory();

INSERT INTO Dates VALUES (to_datetime64('2024-06-09 13:28:00.000',3 ,'Europe/London'), to_time('6/9/2024 13:28','Europe/London'), to_time('6/9/2024 13:28','Europe/London'));

SELECT t1, t2, t3 FROM Dates;

DROP STREAM IF EXISTS Dates;
