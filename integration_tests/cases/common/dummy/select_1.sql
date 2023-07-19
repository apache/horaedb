SELECT 1;

SELECT x;

SELECT 'a';

SELECT NOT(1=1);

-- Revert to return error in https://github.com/apache/arrow-datafusion/pull/6599
SELECT NOT(1);

SELECT TRUE;

SELECT FALSE;

SELECT NOT(TRUE);

SELECT 10 - 2 * 3;

SELECT (10 - 2) * 3;

-- FIXME
SELECT "That is not good.";

SELECT *;
