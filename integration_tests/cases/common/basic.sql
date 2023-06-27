DROP TABLE IF EXISTS `demo`;

CREATE TABLE demo (
    name string TAG,
    value double NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    enable_ttl = 'false'
);


INSERT INTO demo (t, name, value)
    VALUES (1651737067000, 'ceresdb', 100);


SELECT * FROM demo;

INSERT INTO demo (t, name, value)
    VALUES (1651737067001, "ceresdb", 100);

SELECT * FROM demo;

DROP TABLE IF EXISTS `demo`;

CREATE TABLE `DeMo` (
    `nAmE` string TAG,
    value double NOT NULL,
    t timestamp NOT NULL,
    timestamp KEY (t)) ENGINE = Analytic WITH (
    enable_ttl = 'false'
);


SELECT `nAmE` FROM `DeMo`;

DROP TABLE `DeMo`;

DROP TABLE IF EXISTS `binary_demo`;

CREATE TABLE `binary_demo` (
    `name` string TAG,
    `value` varbinary NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t)) ENGINE=Analytic WITH (
    enable_ttl = 'false'
);

INSERT INTO binary_demo(t, name, value) VALUES(1667374200022, 'ceresdb', x'11');

SELECT * FROM binary_demo WHERE value = x'11';

DROP TABLE `binary_demo`;
