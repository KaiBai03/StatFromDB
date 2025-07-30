--P波传输时延
DROP TABLE IF EXISTS public.P_send_time;

CREATE TABLE public.P_send_time AS
SELECT
    'P波预警传输时间统计≤0.1s' AS "项目",
    count(*) AS "总数",
    count(*) FILTER (
        where p_send_time <= 0.1
    ) as "合格数",
    COALESCE(
        ROUND(
            100.0 * count(*) FILTER (
                where p_send_time <= 0.1
            ) / NULLIF(count(*), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥95%' AS "标准",
    CASE
        WHEN NULLIF(count(*), 0) IS NULL THEN NULL
        WHEN count(*) FILTER (
            where p_send_time <= 0.1
        )::numeric / NULLIF(count(*), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    round(max(p_send_time), 3) AS "最大值",
    round(min(p_send_time), 3) AS "最小值",
    round(avg(p_send_time), 3) AS "平均值"
FROM public.p_compare_result;

--P波预警震中位置偏差
DROP TABLE IF EXISTS public.P_epicenter_deviation;

CREATE TABLE public.P_epicenter_deviation AS
SELECT
    'P波预警震中位置偏差' AS "项目",
    COUNT(*) AS "总数",
    SUM(
        CASE WHEN epicenter_deviation_km <= 60 THEN 1 ELSE 0 END
    ) AS "偏差≤60km组数",
    COALESCE(
        ROUND(
            100.0 * SUM(CASE WHEN epicenter_deviation_km <= 60 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "偏差≤60km占比",
    '≥60%' AS "偏差≤60km占比标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN ROUND(
            100.0 * SUM(CASE WHEN epicenter_deviation_km <= 60 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) >= 60 THEN '是'
        ELSE '否'
    END AS "是否达标(60km)",
    SUM(
        CASE WHEN epicenter_deviation_km <= 100 THEN 1 ELSE 0 END
    ) AS "偏差≤100km组数",
    COALESCE(
        ROUND(
            100.0 * SUM(CASE WHEN epicenter_deviation_km <= 100 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "偏差≤100km占比",
    '≥80%' AS "偏差≤100km占比标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN ROUND(
            100.0 * SUM(CASE WHEN epicenter_deviation_km <= 100 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            2
        ) >= 80 THEN '是'
        ELSE '否'
    END AS "是否达标(100km)",
    MAX(epicenter_deviation_km) AS "偏差最大值",
    CASE
        WHEN MAX(epicenter_deviation_km) <= 300 THEN '是'
        ELSE '否'
    END AS "最大值不超过300km"
FROM public.p_compare_result;

--P波预警首报判别时间≤3s
DROP TABLE IF EXISTS public.P_judge_time;

CREATE TABLE public.P_judge_time AS
SELECT
    '台站首报P波预警判别时间≤3秒' AS "项目",
    COUNT(*) AS "总数",
    COUNT(*) FILTER (
        WHERE p_wave_judge_time_sta <= 3
    ) AS "合格数",
    COALESCE(
        ROUND(
            COUNT(*) FILTER (
                WHERE p_wave_judge_time_sta <= 3
            )::numeric / NULLIF(COUNT(*), 0)::numeric * 100,
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥90%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE p_wave_judge_time_sta <= 3
        )::numeric / NULLIF(COUNT(*), 0)::numeric >= 0.9 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(p_wave_judge_time_sta), 3) AS "平均值"
FROM public.p_compare_result;

--P波预警震级偏差≤1
DROP TABLE IF EXISTS public.P_mag_deviation;

CREATE TABLE public.P_mag_deviation AS
SELECT
    'P波预警震级偏差≤1' AS "项目",
    count(*) AS "总数",
    count(*) FILTER (
        where magnitude_diff <= 1
    ) as "合格数",
    COALESCE(
        ROUND(
            100.0 * count(*) FILTER (
                where magnitude_diff <= 1
            ) / NULLIF(count(*), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥95%' AS "标准",
    CASE
        WHEN NULLIF(count(*), 0) IS NULL THEN NULL
        WHEN ROUND(
            100.0 * count(*) FILTER (
                where magnitude_diff <= 1
            ) / NULLIF(count(*), 0),
            2
        ) >= 50 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(magnitude_diff), 3) AS "偏差平均值"
FROM public.p_compare_result;

--P波预警漏报
DROP TABLE IF EXISTS public.P_warning_miss;

CREATE TABLE public.P_warning_miss AS
SELECT
    'P波预警漏报' AS "项目",
    COUNT(*) AS "总数",
    COUNT(*) FILTER (
        WHERE first_jk_time IS NULL
    ) AS "漏报数",
    COALESCE(
        ROUND(
            COUNT(*) FILTER (
                WHERE first_jk_time IS NULL
            )::numeric / NULLIF(COUNT(*), 0)::numeric * 100,
            2
        ) || '%',
        'NAN%'
    ) AS "漏报率",
    '≤5%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE first_jk_time IS NULL
        )::numeric / NULLIF(COUNT(*), 0)::numeric * 100 <= 5 THEN '是'
        ELSE '否'
    END AS "是否达标"
FROM public.p_matched_details;

--先报警后预警数量
DROP TABLE IF EXISTS public.s_alarm_before_p;

CREATE TABLE public.s_alarm_before_p AS
SELECT '先报警后预警' AS "项目", COUNT(*) FILTER (
        WHERE p.first_jk_time > s.first_jk_time
    ) AS "先报警后预警数量"
FROM public.p_matched_details p
    LEFT JOIN public.s_matched_details s ON p.id = s.id;

--阈值报警传输时延≤0.1s(40gal)
DROP TABLE IF EXISTS public.S_40gal_send_time;

CREATE TABLE public.S_40gal_send_time AS
SELECT
    '阈值报警传输时延≤0.1s(40gal)' AS "项目",
    COUNT(*) AS "总数",
    COUNT(*) FILTER (
        WHERE s_send_time < 0.1
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE s_send_time < 0.1
            ) / NULLIF(COUNT(*), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '95%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE s_send_time < 0.1
        )::numeric / NULLIF(COUNT(*), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(s_send_time)::NUMERIC, 3) AS "平均值",
    MAX(s_send_time) AS "最大值",
    MIN(s_send_time) AS "最小值"
FROM public.s_compare_result;

--阈值报警传输时延≤0.1s(80gal)
DROP TABLE IF EXISTS public.S_80gal_send_time;

CREATE TABLE public.S_80gal_send_time AS
SELECT
    '阈值报警传输时延≤0.1s(80gal)' AS "项目",
    COUNT(gal80_send_time) AS "总数",
    COUNT(*) FILTER (
        WHERE gal80_send_time <= 0.1
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE gal80_send_time < 0.1
            ) / NULLIF(COUNT(gal80_send_time), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '95%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(gal80_send_time), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE gal80_send_time <= 0.1
        )::numeric / NULLIF(COUNT(gal80_send_time), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(gal80_send_time)::numeric, 3) AS "平均值",
    MAX(gal80_send_time) AS "最大值",
    MIN(gal80_send_time) AS "最小值"
FROM public.s_compare_result
WHERE gal80_send_time IS NOT NULL;

--阈值报警传输时延≤0.1s(120gal)
DROP TABLE IF EXISTS public.S_120gal_send_time;

CREATE TABLE public.S_120gal_send_time AS
SELECT
    '阈值报警传输时延≤0.1s(120gal)' AS "项目",
    COUNT(gal120_send_time) AS "总数",
    COUNT(*) FILTER (
        WHERE gal120_send_time <= 0.1
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE gal120_send_time < 0.1
            ) / NULLIF(COUNT(gal120_send_time), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '95%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(gal120_send_time), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE gal120_send_time <= 0.1
        )::numeric / NULLIF(COUNT(gal120_send_time), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(gal120_send_time)::numeric, 3) AS "平均值",
    MAX(gal120_send_time) AS "最大值",
    MIN(gal120_send_time) AS "最小值"
FROM public.s_compare_result
WHERE gal120_send_time IS NOT NULL;

--阈值报警漏报
DROP TABLE IF EXISTS public.S_warning_miss;

CREATE TABLE public.S_warning_miss AS
SELECT
    'S波预警漏报' AS "项目",
    COUNT(*) AS "总数",
    COUNT(*) FILTER (
        WHERE first_jk_time IS NULL
    ) AS "漏报数",
    COALESCE(
        ROUND(
            COUNT(*) FILTER (
                WHERE first_jk_time IS NULL
            )::numeric / NULLIF(COUNT(*), 0)::numeric * 100,
            2
        ) || '%',
        'NAN%'
    ) AS "漏报率",
    '≤0%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE first_jk_time IS NULL
        )::numeric / NULLIF(COUNT(*), 0)::numeric <= 0 THEN '是'
        ELSE '否'
    END AS "是否达标"
FROM public.s_matched_details;

--阈值报警最大偏差≤5%
DROP TABLE IF EXISTS public.S_peak_deviation;

CREATE TABLE public.S_peak_deviation AS
SELECT
    '阈值报警最大偏差≤5%' AS "项目",
    COUNT(*) FILTER (
        WHERE stand.actual_peak >= 40
    ) AS "总数",
    COUNT(*) FILTER (
        WHERE scr.peak_deviation_percent <= 5
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE scr.peak_deviation_percent <= 5
            ) / NULLIF(
                COUNT(*) FILTER (
                    WHERE stand.actual_peak >= 40
                ),
                0
            ),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥95%' AS "标准",
    CASE
        WHEN NULLIF(
            COUNT(*) FILTER (
                WHERE stand.actual_peak >= 40
            ),
            0
        ) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE scr.peak_deviation_percent <= 5
        )::numeric / NULLIF(
            COUNT(*) FILTER (
                WHERE stand.actual_peak >= 40
            ),
            0
        ) >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    MAX(scr.peak_deviation_percent) || '%' AS "最大偏差"
FROM public.stand_answer stand
    LEFT JOIN public.s_compare_result scr ON stand.id = scr.id;

--阈值报警判别时延≤0.5s(40gal)
DROP TABLE IF EXISTS public.S_40gal_judge_time;

CREATE TABLE public.S_40gal_judge_time AS
SELECT
    '阈值报警判别时延≤0.5s(40gal)' AS "项目",
    COUNT(*) as "总数",
    COUNT(*) FILTER (
        WHERE swave_judge_time_station <= 0.5
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE swave_judge_time_station <= 0.5
            ) / NULLIF(COUNT(*), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥70%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE swave_judge_time_station <= 0.5
        )::numeric / NULLIF(COUNT(*), 0)::numeric >= 0.7 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(swave_judge_time_station), 3) AS "平均值",
    MAX(swave_judge_time_station) as "最大值",
    MIN(swave_judge_time_station) as "最小值"
FROM public.s_compare_result;

--阈值报警判别时延≤0.5s(80gal)
DROP TABLE IF EXISTS public.S_80gal_judge_time;

CREATE TABLE public.S_80gal_judge_time AS
SELECT
    '阈值报警判别时延≤0.5s(80gal)' AS "项目",
    COUNT(gal80_judge_time_station) as "总数",
    COUNT(*) FILTER (
        WHERE gal80_judge_time_station <= 0.5
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE gal80_judge_time_station <= 0.5
            ) / NULLIF(COUNT(gal80_judge_time_station), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥70%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(gal80_judge_time_station), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE gal80_judge_time_station <= 0.5
        )::numeric / NULLIF(COUNT(gal80_judge_time_station), 0)::numeric >= 0.7 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(gal80_judge_time_station), 3) AS "平均值",
    MAX(gal80_judge_time_station) as "最大值",
    MIN(gal80_judge_time_station) as "最小值"
FROM public.s_compare_result
WHERE gal80_judge_time_station IS NOT NULL;

DROP TABLE IF EXISTS public.S_120gal_judge_time;

CREATE TABLE public.S_120gal_judge_time AS
SELECT
    '阈值报警判别时延≤0.5s(120gal)' AS "项目",
    COUNT(gal120_judge_time_station) as "总数",
    COUNT(*) FILTER (
        WHERE gal120_judge_time_station <= 0.5
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE gal120_judge_time_station <= 0.5
            ) / NULLIF(COUNT(gal120_judge_time_station), 0),
            2
        ) || '%',
        'NAN%'
    ) AS "合格率",
    '≥70%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(gal120_judge_time_station), 0) IS NULL THEN NULL
        WHEN COUNT(*) FILTER (
            WHERE gal120_judge_time_station <= 0.5
        )::numeric / NULLIF(COUNT(gal120_judge_time_station), 0)::numeric >= 0.7 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(gal120_judge_time_station), 3) AS "平均值",
    MAX(gal120_judge_time_station) as "最大值",
    MIN(gal120_judge_time_station) as "最小值"
FROM public.s_compare_result
WHERE gal120_judge_time_station IS NOT NULL;