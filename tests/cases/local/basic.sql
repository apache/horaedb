CREATE TABLE demo (name string TAG, value double NOT NULL, t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='false');

INSERT INTO demo(t, name, value) VALUES(1651737067000, 'ceresdb', 100);

SELECT * FROM demo;

DROP TABLE demo;