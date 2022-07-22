DROP TABLE `04_explain_t`;

CREATE TABLE `04_explain_t` (t timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic;

EXPLAIN SELECT t FROM `04_explain_t`;

DROP TABLE `04_explain_t`;