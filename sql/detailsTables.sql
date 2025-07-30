DROP TABLE IF EXISTS public.details;

CREATE TABLE public.details AS
SELECT
    test_id AS 实验编号,
    waveform_id AS 波形编号,
    st.send_time AS 波形发送时间,
    end_time AS 波形结束时间,
    station_name AS 台站编码,
    earthquake_type AS 波形类型,
    s_warning_miss AS 漏报S波,
    CASE
        WHEN pmd.first_jk_time > smd.first_jk_time THEN 'xs_hp'
        ELSE p_warning_miss
    END AS 漏报P波,
    actual_magnitude AS 实际震级,
    earthquake_level AS 首报震级,
    magnitude_diff AS 震级偏差,
    actual_longitude AS 实际经度,
    actual_latitude AS 实际纬度,
    source_longitude AS 首报经度,
    source_latitude AS 首报纬度,
    epicenter_deviation_km AS "震中偏差(km)",
    p_wave_judge_time AS "P波预警判别时间(s)",
    p_wave_judge_time_sta AS "P波判别预警时间-台站(s)",
    p_wave_time AS 实际P波初至时间,
    pmd.first_rcv_jktime AS P波报警时间,
    actual_peak AS "实际峰值(gal)",
    wave_peak AS "首报峰值(gal)",
    peak_deviation AS "峰值偏差(gal)",
    peak_deviation_percent AS 峰值偏差百分比,
    swave_judge_time AS "阈值报警判别时间(s)",
    swave_judge_time_station AS "阈值判别时间-台站(s)",
    actual_distance AS "实际震中距(km)",
    epi_dist AS "首报震中距(km)",
    epi_dist_diff AS "震中距偏差(km)",
    azimuth AS 实际方位角,
    azi_angle AS 首报方位角,
    ROUND(
        ABS(azi_angle - azimuth)::NUMERIC,
        3
    ) AS 方位角偏差,
    gal80_judge_time AS "80gal阈值报警判别时间(s)",
    gal80_judge_time_station AS "80gal阈值报警判别时间-台站(s)",
    gal120_judge_time AS "120gal阈值报警判别时间(s)",
    gal120_judge_time_station AS "120gal阈值报警判别时间-台站(s)",
    p_send_time AS "P波预警首报传输时间(s)",
    s_send_time AS "阈值报警首报传输时间(s)",
    gal80_send_time AS "80gal阈值报警传输时间(s)",
    gal120_send_time AS "120gal阈值报警传输时间(s)"
FROM
    public.standanswer_p_wave_alarm st
    LEFT JOIN public.p_compare_result pcr ON st.id = pcr.id
    LEFT JOIN public.s_compare_result scr ON st.id = scr.id
    LEFT JOIN public.p_matched_details pmd ON st.id = pmd.id
    LEFT JOIN public.s_matched_details smd ON st.id = smd.id
ORDER BY st.id;