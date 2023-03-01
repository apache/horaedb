DROP TABLE IF EXISTS issue59;

CREATE TABLE issue59 (
    ts timestamp NOT NULL,
    id int,
    account string,
    timestamp KEY (ts)) ENGINE=Analytic
WITH(
	 enable_ttl='false'
);

SELECT id+1, count(distinct(account))
FROM issue59
GROUP BY id+1;

explain SELECT id+1, count(distinct(account))
FROM issue59
GROUP BY id+1;
