CREATE TABLE `issue_1087` (
    `name` string TAG NULL,
    `value` double NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t))
 ENGINE=Analytic with (enable_ttl='false');


-- Check which optimizer rules we are using now
explain verbose select * from issue_1087;

DROP TABLE `issue_1087`;
