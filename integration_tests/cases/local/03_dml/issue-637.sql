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

SELECT * FROM `issue637`;

-- Test all data types mentioned in our user document.
CREATE TABLE IF NOT EXISTS `issue637_1`
(
    t timestamp NOT NULL,
    double_filed double,
    float_filed float,
    str_field string,
    var_field varbinary,
    u64_field uint64,
    u32_field uint32,
    u16_field uint16,
    u8_field uint8,
    i64_field int64,
    i32_field int32,
    i16_field int16,
    i8_field int8,
    bool_field boolean,
    TIMESTAMP KEY (t)
) ENGINE=Analytic with(enable_ttl = 'false');

INSERT INTO issue637_1
    (`t`,`double_filed`,`float_filed`,`str_field`,`var_field`,`u64_field`,`u32_field`,`u16_field`,`u8_field`,`i64_field`,`i32_field`,`i16_field`,`i8_field`,`bool_field`)
VALUES
    (1651737067000,100,100,"s","v",100,100,100,100,100,100,100,100,false);

SELECT * FROM `issue637_1`;
