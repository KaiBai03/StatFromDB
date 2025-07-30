ALTER TABLE public.standanswer_p_wave_alarm
DROP COLUMN IF EXISTS next_send_time;

ALTER TABLE public.standanswer_p_wave_alarm
ADD COLUMN next_send_time timestamp;

WITH
    next_times AS (
        SELECT id, LEAD(send_time) OVER (
                ORDER BY send_time
            ) AS next_send_time, end_time
        FROM public.standanswer_p_wave_alarm
    )
UPDATE public.standanswer_p_wave_alarm t
SET
    next_send_time = COALESCE(n.next_send_time, n.end_time)
FROM next_times n
WHERE
    t.id = n.id;

DROP TABLE IF EXISTS public.stand_answer;

CREATE TABLE public.stand_answer AS
SELECT
    id,
    test_id,
    waveform_id,
    station_name,
    station_manager,
    start_time,
    send_time,
    end_time,
    duration,
    earthquake_type,
    distance_class actual_depth,
    actual_magnitude,
    actual_distance,
    actual_latitude,
    actual_longitude,
    actual_peak,
    p_wave_index,
    p_wave_time,
    s_wave_first_index,
    s_wave_first_time,
    s_wave_first_peak,
    s_wave_second_index,
    s_wave_second_time,
    s_wave_second_peak,
    s_wave_third_index,
    s_wave_third_time,
    s_wave_third_peak,
    azimuth,
    next_send_time
FROM public.standanswer_p_wave_alarm;

DROP TABLE IF EXISTS public.P_matched_details;

CREATE TABLE public.P_matched_details AS
SELECT
    stand.id,
    stand.send_time,
    stand.next_send_time,
    p_min.jk_time AS first_jk_time,
    p_min.rcv_jktime AS first_rcv_jktime,
    p_min.sta_code,
    p_min.device_code,
    p_min.source_longitude,
    p_min.source_latitude,
    p_min.epi_dist,
    p_min.azi_angle,
    p_min.earthquake_level
FROM public.stand_answer stand
    LEFT JOIN LATERAL (
        SELECT
            jk_time, rcv_jktime, sta_code, device_code, ROUND(source_longitude::numeric, 3) AS source_longitude, ROUND(source_latitude::numeric, 3) AS source_latitude, ROUND(epi_dist::numeric, 3) AS epi_dist, ROUND(azi_angle::numeric, 3) AS azi_angle, ROUND(earthquake_level::numeric, 3) AS earthquake_level
        FROM station_p_wave_alarm p
        WHERE
            p.jk_time BETWEEN stand.send_time AND stand.next_send_time
        ORDER BY jk_time ASC
        LIMIT 1
    ) p_min ON TRUE
ORDER BY stand.id;

DROP TABLE IF EXISTS public.P_compare_result;

CREATE TABLE public.P_compare_result AS
SELECT
    stand.id,
    stand.send_time,
    -- P波漏报
    stand.next_send_time,
    CASE
        WHEN pmd.first_jk_time is NULL THEN '1'
        ELSE NULL
    END AS p_warning_miss,
    -- 震级偏差（保留两位小数）
    ROUND(
        ABS(
            pmd.earthquake_level - stand.actual_magnitude
        )::NUMERIC,
        2
    ) AS magnitude_diff,
    -- Haversine公式计算震中偏差（单位：km，保留3位小数）
    ROUND(
        (
            2 * 6371 * asin(
                sqrt(
                    power(
                        sin(
                            radians(
                                (
                                    pmd.source_latitude - stand.actual_latitude
                                ) / 2
                            )
                        ),
                        2
                    ) + cos(
                        radians(stand.actual_latitude)
                    ) * cos(radians(pmd.source_latitude)) * power(
                        sin(
                            radians(
                                (
                                    pmd.source_longitude - stand.actual_longitude
                                ) / 2
                            )
                        ),
                        2
                    )
                )
            )
        )::numeric,
        3
    ) AS epicenter_deviation_km,
    -- P波判别时间（最早rcv_jktime-答案p_wave_time)
    CASE
        WHEN pmd.first_rcv_jktime IS NOT NULL
        AND stand.p_wave_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        pmd.first_rcv_jktime - stand.p_wave_time
                    )
            )::numeric,
            3
        )
        ELSE NULL
    END AS p_wave_judge_time,
    -- P波判别时间（台站）（首报jk_time-答案p_wave_time)
    CASE
        WHEN pmd.first_jk_time IS NOT NULL
        AND stand.p_wave_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        pmd.first_jk_time - stand.p_wave_time
                    )
            )::numeric,
            3
        )
        ELSE NULL
    END AS p_wave_judge_time_sta,
    -- 震中距偏差（绝对值，保留三位小数）
    ROUND(
        ABS(
            pmd.epi_dist - stand.actual_distance
        )::numeric,
        3
    ) AS epi_dist_diff,
    -- 方位角偏差（绝对值，保留三位小数）
    ROUND(
        ABS(pmd.azi_angle - stand.azimuth)::numeric,
        3
    ) AS azi_angle_diff,
    -- P波预警传输时间
    CASE
        WHEN pmd.first_jk_time IS NOT NULL
        AND pmd.first_rcv_jktime IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        pmd.first_rcv_jktime - pmd.first_jk_time
                    )
            )::numeric,
            3
        )
        ELSE NULL
    END AS p_send_time
FROM public.stand_answer stand
    LEFT JOIN public.P_matched_details pmd ON stand.id = pmd.id
ORDER BY stand.id;

DROP TABLE IF EXISTS public.S_matched_details;

CREATE TABLE public.S_matched_details AS
SELECT
    stand.id,
    stand.send_time,
    stand.next_send_time,
    s_min.jk_time AS first_jk_time,
    s_min.rcv_jktime AS first_rcv_jktime,
    -- 80gal首次达到的jk_time和rcv_jktime
    gal80.jk_time AS gal80_jk_time,
    gal80.rcv_jktime AS gal80_rcv_jktime,
    -- 120gal首次达到的jk_time和rcv_jktime
    gal120.jk_time AS gal120_jk_time,
    gal120.rcv_jktime AS gal120_rcv_jktime,
    s_min.sta_code,
    s_min.device_code,
    peak_info.wave_peak
FROM
    public.stand_answer stand
    LEFT JOIN LATERAL (
        SELECT
            jk_time,
            rcv_jktime,
            sta_code,
            device_code
        FROM station_s_wave_alarm s
        WHERE
            s.jk_time BETWEEN stand.send_time AND stand.next_send_time
        ORDER BY jk_time ASC
        LIMIT 1
    ) s_min ON TRUE
    LEFT JOIN LATERAL (
        SELECT jk_time, rcv_jktime
        FROM station_s_wave_alarm s
        WHERE
            s.jk_time BETWEEN stand.send_time AND stand.next_send_time
            AND x_acc_value >= 80
        ORDER BY jk_time ASC
        LIMIT 1
    ) gal80 ON TRUE
    LEFT JOIN LATERAL (
        SELECT jk_time, rcv_jktime
        FROM station_s_wave_alarm s
        WHERE
            s.jk_time BETWEEN stand.send_time AND stand.next_send_time
            AND x_acc_value >= 120
        ORDER BY jk_time ASC
        LIMIT 1
    ) gal120 ON TRUE
    LEFT JOIN LATERAL (
        SELECT ROUND(MAX(x_acc_value)::NUMERIC, 3) AS wave_peak
        FROM station_s_wave_alarm s
        WHERE
            s.jk_time BETWEEN stand.send_time AND stand.next_send_time
    ) peak_info ON TRUE
WHERE
    stand.actual_peak >= 40
ORDER BY stand.id;

DROP TABLE IF EXISTS public.s_compare_result;

CREATE TABLE public.S_compare_result AS
SELECT
    stand.id,
    stand.send_time,
    stand.next_send_time,
    -- S波漏报
    CASE
        WHEN smd.first_jk_time IS NULL THEN '1'
        ELSE NULL
    END AS s_warning_miss,
    -- S波首报判别时间
    CASE
        WHEN smd.first_rcv_jktime IS NOT NULL
        AND stand.s_wave_first_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        smd.first_rcv_jktime - stand.s_wave_first_time
                    )
            ),
            3
        )
        ELSE NULL
    END AS swave_judge_time,
    -- S波首报判别时间（台站）
    CASE
        WHEN smd.first_jk_time IS NOT NULL
        AND stand.s_wave_first_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        smd.first_jk_time - stand.s_wave_first_time
                    )
            ),
            3
        )
        ELSE NULL
    END AS swave_judge_time_station,
    -- 80gal判别时间
    CASE
        WHEN smd.gal80_rcv_jktime IS NOT NULL
        AND stand.s_wave_second_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        smd.gal80_rcv_jktime - stand.s_wave_second_time
                    )
            ),
            3
        )
        ELSE NULL
    END AS gal80_judge_time,
    -- 80gal判别时间(台站)
    CASE
        WHEN smd.gal80_jk_time IS NOT NULL
        AND stand.s_wave_second_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        smd.gal80_jk_time - stand.s_wave_second_time
                    )
            ),
            3
        )
        ELSE NULL
    END AS gal80_judge_time_station,
    -- 120gal判别时间
    CASE
        WHEN smd.gal120_rcv_jktime IS NOT NULL
        AND stand.s_wave_third_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        smd.gal120_rcv_jktime - stand.s_wave_third_time
                    )
            ),
            3
        )
        ELSE NULL
    END AS gal120_judge_time,
    -- 120gal判别时间(台站)
    CASE
        WHEN smd.gal120_jk_time IS NOT NULL
        AND stand.s_wave_third_time IS NOT NULL THEN ROUND(
            EXTRACT(
                EPOCH
                FROM (
                        smd.gal120_jk_time - stand.s_wave_third_time
                    )
            ),
            3
        )
        ELSE NULL
    END AS gal120_judge_time_station,
    -- 阈值报警最大值误差
    ROUND(
        (
            ABS(
                stand.actual_peak - smd.wave_peak
            )
        )::NUMERIC,
        3
    ) AS peak_deviation,
    -- 误差百分比
    CASE
        WHEN stand.actual_peak IS NOT NULL
        AND stand.actual_peak <> 0 THEN ROUND(
            (
                ABS(
                    stand.actual_peak - smd.wave_peak
                ) / stand.actual_peak
            )::NUMERIC * 100,
            2
        )
        ELSE NULL
    END AS peak_deviation_percent,
    -- 阈值报警传输时间
    ROUND(
        EXTRACT(
            EPOCH
            FROM (
                    smd.first_rcv_jktime - smd.first_jk_time
                )
        )::numeric,
        3
    ) AS s_send_time,
    ROUND(
        EXTRACT(
            EPOCH
            FROM (
                    smd.gal80_rcv_jktime - smd.gal80_jk_time
                )
        )::numeric,
        3
    ) AS gal80_send_time,
    ROUND(
        EXTRACT(
            EPOCH
            FROM (
                    smd.gal120_rcv_jktime - smd.gal120_jk_time
                )
        )::numeric,
        3
    ) AS gal120_send_time
FROM public.stand_answer stand
    INNER JOIN public.s_matched_details smd ON stand.id = smd.id;

