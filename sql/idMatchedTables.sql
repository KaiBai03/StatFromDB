-- 刷新 id_matched_p_wave_info 表：只保留 earth_id, jk_time, rcv_jktime, send_time
DROP TABLE IF EXISTS public.id_matched_p_wave_info;

CREATE TABLE public.id_matched_p_wave_info AS
SELECT t.earth_id, t.jk_time, t.rcv_jktime, ROUND(
        EXTRACT(
            EPOCH
            FROM (t.rcv_jktime - t.jk_time)
        ), 3
    ) AS send_time
FROM (
        SELECT *, ROW_NUMBER() OVER (
                PARTITION BY
                    earth_id
                ORDER BY jk_time ASC
            ) AS rn
        FROM station_p_wave_alarm
    ) t
WHERE
    t.rn = 1;

-- 刷新 id_matched_s_wave_info 表：包含三组 jk_time/rcv_jktime 及三组 send_time
DROP TABLE IF EXISTS public.id_matched_s_wave_info;

CREATE TABLE public.id_matched_s_wave_info AS
SELECT
    s.earth_id,
    -- 最早 jk_time 和 rcv_jktime
    first_row.jk_time AS jk_time,
    first_row.rcv_jktime AS rcv_jktime,
    ROUND(
        EXTRACT(
            EPOCH
            FROM (
                    first_row.rcv_jktime - first_row.jk_time
                )
        ),
        3
    ) AS send_time,
    -- x_acc_value≥80 最早 jk_time 和 rcv_jktime
    gal80_row.jk_time AS gal80_jk_time,
    gal80_row.rcv_jktime AS gal80_rcv_jktime,
    ROUND(
        EXTRACT(
            EPOCH
            FROM (
                    gal80_row.rcv_jktime - gal80_row.jk_time
                )
        ),
        3
    ) AS gal80_send_time,
    -- x_acc_value≥120 最早 jk_time 和 rcv_jktime
    gal120_row.jk_time AS gal120_jk_time,
    gal120_row.rcv_jktime AS gal120_rcv_jktime,
    ROUND(
        EXTRACT(
            EPOCH
            FROM (
                    gal120_row.rcv_jktime - gal120_row.jk_time
                )
        ),
        3
    ) AS gal120_send_time
FROM (
        SELECT DISTINCT
            earth_id
        FROM station_s_wave_alarm
    ) s
    LEFT JOIN LATERAL (
        SELECT jk_time, rcv_jktime
        FROM station_s_wave_alarm
        WHERE
            earth_id = s.earth_id
        ORDER BY jk_time ASC
        LIMIT 1
    ) first_row ON TRUE
    LEFT JOIN LATERAL (
        SELECT jk_time, rcv_jktime
        FROM station_s_wave_alarm
        WHERE
            earth_id = s.earth_id
            AND x_acc_value >= 80
        ORDER BY jk_time ASC
        LIMIT 1
    ) gal80_row ON TRUE
    LEFT JOIN LATERAL (
        SELECT jk_time, rcv_jktime
        FROM station_s_wave_alarm
        WHERE
            earth_id = s.earth_id
            AND x_acc_value >= 120
        ORDER BY jk_time ASC
        LIMIT 1
    ) gal120_row ON TRUE;

