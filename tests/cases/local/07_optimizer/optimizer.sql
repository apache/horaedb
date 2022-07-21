DROP TABLE `07_optimizer_t`;

CREATE TABLE `07_optimizer_t` (name string TAG, value double NOT NULL, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='false');

-- SHOW CREATE TABLE `07_optimizer_t`;

-- EXPLAIN SELECT max(value) AS c1, avg(value) AS c2 FROM `07_optimizer_t` GROUP BY name;

DROP TABLE `07_optimizer_t`;