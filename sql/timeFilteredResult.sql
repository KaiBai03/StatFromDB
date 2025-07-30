DROP TABLE IF EXISTS public.time_filtered_P_send_time;

CREATE TABLE public.time_filtered_P_send_time AS
SELECT
    'P波预警传输时间统计≤0.1s' AS "项目",
    count(*) AS "总数",
    count(*) FILTER (
        where
            send_time <= 0.1
    ) as "合格数",
    COALESCE(
        ROUND(
            100.0 * count(*) FILTER (
                where
                    send_time <= 0.1
            ) / NULLIF(count(*), 0),
            2
        ) || '%',
        'NaN%'
    ) AS "合格率",
    '≥95%' AS "标准",
    CASE
        WHEN NULLIF(count(*), 0) IS NULL THEN 'NULL'
        WHEN count(*) FILTER (
            where
                send_time <= 0.1
        )::numeric / NULLIF(count(*), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    round(max(send_time), 3) AS "最大值",
    round(min(send_time), 3) AS "最小值",
    round(avg(send_time), 3) AS "平均值"
FROM public.id_matched_p_filtered_by_time;

DROP TABLE IF EXISTS public.time_filtered_S_40gal_send_time;

CREATE TABLE public.time_filtered_S_40gal_send_time AS
SELECT
    '阈值报警传输时延≤0.1s(40gal)' AS "项目",
    COUNT(*) AS "总数",
    COUNT(*) FILTER (
        WHERE
            send_time < 0.1
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE
                    send_time < 0.1
            ) / NULLIF(COUNT(*), 0),
            2
        ) || '%',
        'NaN%'
    ) AS "合格率",
    '95%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(*), 0) IS NULL THEN 'NULL'
        WHEN COUNT(*) FILTER (
            WHERE
                send_time < 0.1
        )::numeric / NULLIF(COUNT(*), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(AVG(send_time)::NUMERIC, 3) AS "平均值",
    MAX(send_time) AS "最大值",
    MIN(send_time) AS "最小值"
FROM public.id_matched_s_filtered_by_time;

--阈值报警传输时延≤0.1s(80gal)
DROP TABLE IF EXISTS public.time_filtered_S_80gal_send_time;

CREATE TABLE public.time_filtered_S_80gal_send_time AS
SELECT
    '阈值报警传输时延≤0.1s(80gal)' AS "项目",
    COUNT(gal80_send_time) AS "总数",
    COUNT(*) FILTER (
        WHERE
            gal80_send_time <= 0.1
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE
                    gal80_send_time < 0.1
            ) / NULLIF(COUNT(gal80_send_time), 0),
            2
        ) || '%',
        'NaN%'
    ) AS "合格率",
    '95%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(gal80_send_time), 0) IS NULL THEN 'NULL'
        WHEN COUNT(*) FILTER (
            WHERE
                gal80_send_time <= 0.1
        )::numeric / NULLIF(COUNT(gal80_send_time), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(
        AVG(gal80_send_time)::numeric,
        3
    ) AS "平均值",
    MAX(gal80_send_time) AS "最大值",
    MIN(gal80_send_time) AS "最小值"
FROM public.id_matched_s_filtered_by_time
WHERE
    gal80_send_time IS NOT NULL;

--阈值报警传输时延≤0.1s(120gal)
DROP TABLE IF EXISTS public.time_filtered_S_120gal_send_time;

CREATE TABLE public.time_filtered_S_120gal_send_time AS
SELECT
    '阈值报警传输时延≤0.1s(120gal)' AS "项目",
    COUNT(gal120_send_time) AS "总数",
    COUNT(*) FILTER (
        WHERE
            gal120_send_time <= 0.1
    ) AS "合格数",
    COALESCE(
        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE
                    gal120_send_time < 0.1
            ) / NULLIF(COUNT(gal120_send_time), 0),
            2
        ) || '%',
        'NaN%'
    ) AS "合格率",
    '95%' AS "标准",
    CASE
        WHEN NULLIF(COUNT(gal120_send_time), 0) IS NULL THEN 'NULL'
        WHEN COUNT(*) FILTER (
            WHERE
                gal120_send_time <= 0.1
        )::numeric / NULLIF(COUNT(gal120_send_time), 0)::numeric >= 0.95 THEN '是'
        ELSE '否'
    END AS "是否达标",
    ROUND(
        AVG(gal120_send_time)::numeric,
        3
    ) AS "平均值",
    MAX(gal120_send_time) AS "最大值",
    MIN(gal120_send_time) AS "最小值"
FROM public.id_matched_s_filtered_by_time
WHERE
    gal120_send_time IS NOT NULL;