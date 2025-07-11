import csv
import psycopg2
import chardet
import logging
import time
import os
import configparser
import sys
from datetime import datetime
import argparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('p_wave_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="导入标准答案CSV到数据库")
    parser.add_argument('csv_path', help='CSV文件路径')
    parser.add_argument('--host', help='数据库主机')
    parser.add_argument('--port', type=int, help='数据库端口')
    parser.add_argument('--dbname', help='数据库名')
    parser.add_argument('--user', help='数据库用户名')
    parser.add_argument('--password', help='数据库密码')
    return parser.parse_args()

def load_config():
    # 优先当前工作目录
    config_path = os.path.join(os.getcwd(), 'config.ini')
    if not os.path.exists(config_path):
        # 兼容未打包时
        config_path = os.path.join(os.path.dirname(sys.argv[0]), 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError("config.ini 未找到")
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if 'database' not in config:
            logger.error("配置文件中缺少 [database] 部分")
            raise KeyError("缺少 [database] 部分")
        db_config = {
            'host': config['database'].get('host', 'localhost'),
            'port': config['database'].getint('port', 5432),
            'dbname': config['database'].get('dbname'),
            'user': config['database'].get('user'),
            'password': config['database'].get('password')
        }
        missing = [key for key in ['dbname', 'user'] if not db_config[key]]
        if missing:
            logger.error("配置文件中缺少必要的数据库信息: %s", ", ".join(missing))
            raise ValueError("缺少必要的数据库配置")
        logger.info("已成功加载数据库配置")
        return db_config
    except Exception as e:
        logger.error("加载配置文件时出错: %s", str(e))
        raise

def get_db_config(args):
    if args.host and args.port and args.dbname and args.user and args.password:
        return {
            'host': args.host,
            'port': args.port,
            'dbname': args.dbname,
            'user': args.user,
            'password': args.password
        }
    return load_config()

def detect_file_encoding(file_path):
    """检测文件编码并转换为PostgreSQL识别的格式"""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(50000)
        detection = chardet.detect(raw_data)

        # 使用PostgreSQL兼容的编码名称
        encoding_mapping = {
            'UTF-8': 'UTF8',
            'GB2312': 'GBK',
            'GBK': 'GBK',
            'GB18030': 'GB18030',
            'ISO-8859-1': 'LATIN1',
            'ascii': 'SQL_ASCII',
            'cp1252': 'WIN1252'
        }

        detected_encoding = detection['encoding'].lower() if detection['encoding'] else 'gbk'
        for py_enc, pg_enc in encoding_mapping.items():
            if py_enc.lower() in detected_encoding:
                logger.info(f"检测到编码: {detected_encoding} -> 使用 {pg_enc}")
                return pg_enc

        return 'GBK'  # 默认中文编码
    except Exception as e:
        logger.error(f"编码检测失败: {e}")
        return 'GBK'


def parse_time(time_str):
    """解析时间字符串为datetime对象"""
    if not time_str or str(time_str).strip() in ['', 'null', 'None']:
        return None
    try:
        # 移除不必要的空白
        time_str = time_str.strip()

        # 处理不同的时间格式
        if '.' in time_str:
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
        elif len(time_str) == 19:
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        else:
            # 尝试其他可能格式
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y%m%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'):
                try:
                    return datetime.strptime(time_str, fmt)
                except:
                    continue
            logger.warning(f"无法识别的时间格式: {time_str}")
            return None
    except Exception as e:
        logger.warning(f"时间解析错误: '{time_str}' - {e}")
        return None


def clean_data(row):
    """数据清洗：处理干扰波的特殊情况"""
    if 'earthquake_type' in row and row['earthquake_type'] == '干扰波':
        # 将干扰波的地震相关字段设为空值
        for field in [
            'distance_class', 'actual_depth', 'actual_magnitude',
            'actual_distance', 'actual_latitude', 'actual_longitude',
            'actual_peak', 'p_wave_index', 'p_wave_time',
            's_wave_first_index', 's_wave_first_time', 's_wave_first_peak',
            's_wave_second_index', 's_wave_second_time', 's_wave_second_peak',
            's_wave_third_index', 's_wave_third_time', 's_wave_third_peak',
            'azimuth'
        ]:
            if field in row:
                row[field] = None

    # 将空字符串转换为None
    for key in row:
        if isinstance(row[key], str) and row[key].strip() == '':
            row[key] = None

    return row


def create_table_schema(cursor, full_table_name):
    """创建表和模式（如果不存在）"""
    try:
        # 分离模式名和表名
        schema_name, table_name = full_table_name.split('.') if '.' in full_table_name else ('public', full_table_name)

        # 创建模式
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.info(f"已创建/验证模式: {schema_name}")

        #删除原表
        drop_table_sql=f"DROP TABLE IF EXISTS {full_table_name}"
        cursor.execute(drop_table_sql)

        # 创建表结构
        create_table_sql = f"""
        
        CREATE TABLE  {full_table_name} (
            id SERIAL PRIMARY KEY,
            test_id INTEGER NOT NULL,
            waveform_id VARCHAR(20) NOT NULL,
            station_name VARCHAR(50) NOT NULL,
            station_manager VARCHAR(50) NOT NULL,
            start_time TIMESTAMP(6) NOT NULL,
            send_time TIMESTAMP(6) NOT NULL,
            duration FLOAT NOT NULL,
            end_time TIMESTAMP(6) NOT NULL,
            earthquake_type VARCHAR(20) NOT NULL,
            distance_class VARCHAR(20),
            actual_depth FLOAT,
            actual_magnitude FLOAT,
            actual_distance FLOAT,
            actual_latitude FLOAT,
            actual_longitude FLOAT,
            actual_peak FLOAT,
            p_wave_index INTEGER,
            p_wave_time TIMESTAMP(6),
            s_wave_first_index INTEGER,
            s_wave_first_time TIMESTAMP(6),
            s_wave_first_peak FLOAT,
            s_wave_second_index INTEGER,
            s_wave_second_time TIMESTAMP(6),
            s_wave_second_peak FLOAT,
            s_wave_third_index INTEGER,
            s_wave_third_time TIMESTAMP(6),
            s_wave_third_peak FLOAT,
            azimuth FLOAT
        );
        """

        # 添加表和列注释
        create_comments_sql = f"""
        COMMENT ON TABLE {full_table_name} IS '地震波形标准答案表';
        COMMENT ON COLUMN {full_table_name}.test_id IS '试验序号';
        COMMENT ON COLUMN {full_table_name}.waveform_id IS '波形编号';
        COMMENT ON COLUMN {full_table_name}.station_name IS '台站名称';
        COMMENT ON COLUMN {full_table_name}.station_manager IS '台站长名称';
        COMMENT ON COLUMN {full_table_name}.start_time IS '开始时间';
        COMMENT ON COLUMN {full_table_name}.send_time IS '发送时间';
        COMMENT ON COLUMN {full_table_name}.duration IS '持续时长(S)';
        COMMENT ON COLUMN {full_table_name}.end_time IS '结束时间';
        COMMENT ON COLUMN {full_table_name}.earthquake_type IS '地震类型';
        COMMENT ON COLUMN {full_table_name}.distance_class IS '多台震中距分类';
        COMMENT ON COLUMN {full_table_name}.actual_depth IS '实际震源深度';
        COMMENT ON COLUMN {full_table_name}.actual_magnitude IS '实际震级';
        COMMENT ON COLUMN {full_table_name}.actual_distance IS '实际震中距';
        COMMENT ON COLUMN {full_table_name}.actual_latitude IS '实际震源纬度';
        COMMENT ON COLUMN {full_table_name}.actual_longitude IS '实际震源经度';
        COMMENT ON COLUMN {full_table_name}.actual_peak IS '实际峰值';
        COMMENT ON COLUMN {full_table_name}.p_wave_index IS 'P波初至位置';
        COMMENT ON COLUMN {full_table_name}.p_wave_time IS 'P波初至时间';
        COMMENT ON COLUMN {full_table_name}.s_wave_first_index IS 'S波一报位置';
        COMMENT ON COLUMN {full_table_name}.s_wave_first_time IS 'S波一报时间';
        COMMENT ON COLUMN {full_table_name}.s_wave_first_peak IS 'S波一报峰值';
        COMMENT ON COLUMN {full_table_name}.s_wave_second_index IS 'S波二报位置';
        COMMENT ON COLUMN {full_table_name}.s_wave_second_time IS 'S波二报时间';
        COMMENT ON COLUMN {full_table_name}.s_wave_second_peak IS 'S波二报峰值';
        COMMENT ON COLUMN {full_table_name}.s_wave_third_index IS 'S波三报位置';
        COMMENT ON COLUMN {full_table_name}.s_wave_third_time IS 'S波三报时间';
        COMMENT ON COLUMN {full_table_name}.s_wave_third_peak IS 'S波三报峰值';
        COMMENT ON COLUMN {full_table_name}.azimuth IS '方位角';
        """

        # 执行创建表结构
        cursor.execute(create_table_sql)
        logger.info(f"已创建/验证表结构: {full_table_name}")

        # 尝试添加注释
        try:
            cursor.execute(create_comments_sql)
            logger.info(f"已添加表和列注释")
        except Exception as e:
            logger.warning(f"添加注释时出错: {e}")

        # 验证表是否存在
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logger.error(f"表创建失败: {full_table_name} 不存在！")
            return False

        return True

    except Exception as e:
        logger.error(f"创建表结构时出错: {e}")
        return False





# 完整表名
FULL_TABLE_NAME = 'public.standanswer_p_wave_alarm'

# CSV文件路径
#CSV_PATH = os.path.join(os.path.dirname(__file__), 'StandardAnswersFile0612.csv')
args=parse_args()
CSV_PATH = args.csv_path



# 主函数
def main():
    logger.info(f"收到命令行参数: {sys.argv}")
    logger.info("=== 开始导入P波警报数据到 %s ===", FULL_TABLE_NAME)
    start_time = time.time()

    try:
        # 加载数据库配置
        logger.info("加载数据库配置...")
        DB_CONFIG = get_db_config(args)

        # 连接数据库
        logger.info("连接数据库...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_client_encoding('UTF8')
        conn.autocommit = False
        cursor = conn.cursor()

        # 确保表结构存在
        logger.info("验证表结构是否存在...")
        if not create_table_schema(cursor, FULL_TABLE_NAME):
            logger.error("无法继续导入，表结构创建失败")
            return False, 0

        conn.commit()

        # 检测文件编码
        logger.info("检测CSV文件编码...")
        encoding = detect_file_encoding(CSV_PATH)
        logger.info("将使用编码: %s", encoding)

        # 准备插入SQL
        insert_sql = f"""
        INSERT INTO {FULL_TABLE_NAME} (
            test_id, waveform_id, station_name, station_manager,
            start_time, send_time, duration, end_time,
            earthquake_type, distance_class, actual_depth, actual_magnitude,
            actual_distance, actual_latitude, actual_longitude, actual_peak,
            p_wave_index, p_wave_time,
            s_wave_first_index, s_wave_first_time, s_wave_first_peak,
            s_wave_second_index, s_wave_second_time, s_wave_second_peak,
            s_wave_third_index, s_wave_third_time, s_wave_third_peak,
            azimuth
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s
        )
        """

        # 使用CSV导入
        logger.info("开始CSV导入...")
        with open(CSV_PATH, 'r', encoding=encoding, errors='replace') as f:
            # 创建CSV阅读器
            reader = csv.DictReader(f, fieldnames=[
                'test_id', 'waveform_id', 'station_name', 'station_manager',
                'start_time', 'send_time', 'duration', 'end_time',
                'earthquake_type', 'distance_class', 'actual_depth', 'actual_magnitude',
                'actual_distance', 'actual_latitude', 'actual_longitude', 'actual_peak',
                'p_wave_index', 'p_wave_time',
                's_wave_first_index', 's_wave_first_time', 's_wave_first_peak',
                's_wave_second_index', 's_wave_second_time', 's_wave_second_peak',
                's_wave_third_index', 's_wave_third_time', 's_wave_third_peak',
                'azimuth'
            ])

            # 跳过标题行
            next(reader)

            # 处理批处理
            batch = []
            batch_size = 500
            total_count = 0
            success_count = 0
            error_count = 0

            for row_num, row in enumerate(reader, start=2):  # 从第2行开始（跳过标题）
                try:
                    # 清洗和转换数据
                    row = clean_data(row)

                    # 准备参数值
                    values = (
                        int(row['test_id']) if row['test_id'] and row['test_id'] != '' else None,
                        row['waveform_id'],
                        row['station_name'],
                        row['station_manager'],
                        parse_time(row['start_time']),
                        parse_time(row['send_time']),
                        float(row['duration']) if row['duration'] and row['duration'] != '' else None,
                        parse_time(row['end_time']),
                        row['earthquake_type'],
                        row['distance_class'] if 'distance_class' in row else None,
                        float(row['actual_depth']) if row['actual_depth'] and row['actual_depth'] != '' else None,
                        float(row['actual_magnitude']) if row['actual_magnitude'] and row[
                            'actual_magnitude'] != '' else None,
                        float(row['actual_distance']) if row['actual_distance'] and row[
                            'actual_distance'] != '' else None,
                        float(row['actual_latitude']) if row['actual_latitude'] and row[
                            'actual_latitude'] != '' else None,
                        float(row['actual_longitude']) if row['actual_longitude'] and row[
                            'actual_longitude'] != '' else None,
                        float(row['actual_peak']) if row['actual_peak'] and row['actual_peak'] != '' else None,
                        int(row['p_wave_index']) if row['p_wave_index'] and row['p_wave_index'] != '' else None,
                        parse_time(row['p_wave_time']),
                        int(row['s_wave_first_index']) if row['s_wave_first_index'] and row[
                            's_wave_first_index'] != '' else None,
                        parse_time(row['s_wave_first_time']),
                        float(row['s_wave_first_peak']) if row['s_wave_first_peak'] and row[
                            's_wave_first_peak'] != '' else None,
                        int(row['s_wave_second_index']) if row['s_wave_second_index'] and row[
                            's_wave_second_index'] != '' else None,
                        parse_time(row['s_wave_second_time']),
                        float(row['s_wave_second_peak']) if row['s_wave_second_peak'] and row[
                            's_wave_second_peak'] != '' else None,
                        int(row['s_wave_third_index']) if row['s_wave_third_index'] and row[
                            's_wave_third_index'] != '' else None,
                        parse_time(row['s_wave_third_time']),
                        float(row['s_wave_third_peak']) if row['s_wave_third_peak'] and row[
                            's_wave_third_peak'] != '' else None,
                        float(row['azimuth']) if row['azimuth'] and row['azimuth'] != '' else None
                    )

                    batch.append(values)
                    total_count += 1

                    # 执行批量插入
                    if len(batch) >= batch_size:
                        try:
                            # 批量插入
                            for item in batch:
                                cursor.execute(insert_sql, item)

                            conn.commit()
                            success_count += len(batch)
                            logger.info("已导入 %d 条记录 (总: %d)", len(batch), success_count)
                            batch = []
                        except Exception as e:
                            logger.error("批量插入错误: %s", str(e))
                            conn.rollback()

                            # 尝试逐条插入以识别问题行
                            temp_batch = batch[:]  # 保存当前批处理
                            batch = []  # 清空批处理

                            for item in temp_batch:
                                try:
                                    cursor.execute(insert_sql, item)
                                    conn.commit()
                                    success_count += 1
                                except Exception as e2:
                                    logger.error("单行插入错误 (第%d行): %s",
                                                 row_num - len(temp_batch) + temp_batch.index(item) + 1, str(e2))
                                    error_count += 1
                                    conn.rollback()

                except Exception as e:
                    logger.error("处理行错误 (第%d行): %s | 行内容: %s", row_num, str(e), str(row))
                    error_count += 1
                    continue

            # 处理剩余批处理
            if batch:
                try:
                    for item in batch:
                        cursor.execute(insert_sql, item)
                    conn.commit()
                    success_count += len(batch)
                    logger.info("导入最后 %d 条记录", len(batch))
                except Exception as e:
                    logger.error("最后一批插入错误: %s", str(e))
                    conn.rollback()
                    error_count += len(batch)

        # 创建索引
        try:
            logger.info("创建索引...")
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_pwave_alarm_test_id ON {FULL_TABLE_NAME}(test_id);
                CREATE INDEX IF NOT EXISTS idx_pwave_alarm_station ON {FULL_TABLE_NAME}(station_name);
                CREATE INDEX IF NOT EXISTS idx_pwave_alarm_start_time ON {FULL_TABLE_NAME}(start_time);
                CREATE INDEX IF NOT EXISTS idx_pwave_alarm_type ON {FULL_TABLE_NAME}(earthquake_type);
            """)
            conn.commit()
            logger.info("索引创建完成")
        except Exception as e:
            logger.error("创建索引时出错: %s", str(e))
            conn.rollback()

        # 验证结果
        elapsed_time = time.time() - start_time
        cursor.execute(f"SELECT COUNT(*) FROM {FULL_TABLE_NAME}")
        final_count = cursor.fetchone()[0]

        logger.info("=== 导入完成 ===")
        logger.info("CSV总行数: %d", total_count)
        logger.info("成功导入: %d (成功率: %.2f%%)", success_count,
                    (success_count / total_count) * 100 if total_count > 0 else 0)
        logger.info("失败记录: %d", error_count)
        logger.info("数据库中记录数: %d", final_count)
        logger.info("执行时间: %.2f 秒", elapsed_time)

        if success_count >0:
            return True,success_count
        else:
            logger.error("没有成功导入任何记录，请检查数据和日志")
            return False,0

    except Exception as e:
        logger.error("致命错误: %s", str(e))
        import traceback
        logger.error(traceback.format_exc())
        return False, 0

    finally:
        try:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
                logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error("关闭连接时出错: %s", str(e))


if __name__ == "__main__":
    success, row_count = main()
    if success:
        logger.info("=== P波警报数据导入成功! ===")
        sys.exit(0)
    else:
        logger.error("=== 导入失败，请查看日志 ===")
        sys.exit(1)