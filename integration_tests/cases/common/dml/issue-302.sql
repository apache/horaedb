DROP TABLE IF EXISTS issue302;

CREATE TABLE `issue302` (`name` string TAG NULL, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='false');

INSERT INTO issue302(t,  value) VALUES(1651737067000, 100);

select `t`, count(distinct name) from issue302 group by `t`;

DROP TABLE IF EXISTS issue302;
