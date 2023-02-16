DROP TABLE IF EXISTS `issue637`;

CREATE TABLE IF NOT EXISTS `issue637`
(
    str_tag string TAG,
    int_tag int32 TAG,
    var_tag VARBINARY TAG,
    str_field string,
    int_field int32,
    bin_field string,
    t timestamp NOT NULL,
     TIMESTAMP KEY (t)
) ENGINE=Analytic with(enable_ttl = 'false');


INSERT INTO issue637
    (`str_tag`,`int_tag`,`var_tag`,`str_field`,`int_field`,`bin_field`,`t`)
VALUES
    ("t1",1,"v1","s1",1,"b1",1651737067000);

select * from `issue637`;

