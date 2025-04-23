import os
import re
import json
import time
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.pool
import psycopg2.extras
import pandas as pd
import numpy as np
from collections import defaultdict, Counter

from click.formatting import iter_rows
from matplotlib.artist import kwdoc
from matplotlib.backend_tools import cursors

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger(__name__, "logs/trend_analyzer.log")

# Tạo thread-local storage cho các resource dùng chung
thread_local = threading.local()

class TrendAnalyzer:
    """Class phân tích xu hướng từ dữ liệu Reddit theo thời gian"""
    def __init__(self, min_conn=3, max_conn=10):
        """
            Khởi tạo TrendAnalyzer với connection pool

            Args:
                min_conn (int): Số kết nối tối thiểu trong pool
                max_conn (int): Số kết nối tối đa trong pool
        """
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )

            logger.info(f"Đã khởi tạo connection pool (min={min_conn}, max={max_conn})")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo connection pool: {str(e)}")
            raise

        # Khởi tạo cache cho các kết quả phân tích
        self.cache = {}
        self.cache_lock = threading.Lock()

        logger.info("TrendAnalyzer đã được khởi tạo")

    def get_db_connection(self, max_retries=3, retry_delay=1):
        """
            Lấy kết nối từ connection pool với retry logic

            Args:
                max_retries (int): Số lần thử lại tối đa
                retry_delay (int): Độ trễ giữa các lần thử (giây)

            Returns:
                connection: Kết nối PostgreSQL
        """
        retries = 0
        while retries < max_retries:
            try:
                conn = self.connection_pool.getconn()
                return conn
            except Exception as e:
                retries += 1
                logger.warning(f"Lỗi khi lấy kết nối (lần {retries}): {str(e)}")
                if retries >= max_retries:
                    logger.error("Không thể lấy kết nối sau nhiều lần thử")
                    raise
                time.sleep(retry_delay)

    def return_db_connection(self, conn):
        """
            Trả lại kết nối vào connection pool

            Args:
                conn: Kết nối PostgreSQL cần trả lại
        """
        if conn:
            self.connection_pool.putconn(conn)

    def analyze_weekly_tech_trends(self):
        """
            Phân tích xu hướng công nghệ theo tuần và cập nhật bảng tech_trends

            Returns:
                int: Số lượng xu hướng đã phân tích
        """
        conn = None
        cur = None
        try:
            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            cur = conn.cursor()

            # Tạo temporary table để lưu trữ kết quả phân tích
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_tech_trends AS (
                    SELECT
                        unnest(pa.tech_mentioned) as tech_name,
                        DATE_TRUNC('week', p.created_date) as week_start,
                        p.subreddit_id,
                        COUNT(*) as mention_count,
                        AVG(pa.sentiment_score) as sentiment_avg
                    FROM
                        reddit_data.post_analysis pa
                        JOIN reddit_data.posts p ON pa.post_id = p.post_id
                    WHERE
                        pa.tech_mentioned IS NOT NULL
                    GROUP BY
                        tech_name, week_start, p.subreddit_id
                );
            """)

            # Xóa dữ liệu cũ từ bảng tech_trends
            cur.execute("TRUNCATE TABLE reddit_data.tech_trends RESTART IDENTITY")

            # Chèn dữ liệu mới từ temporary table
            cur.execute("""
                        INSERT INTO reddit_data.tech_trends (
                            tech_name, week_start, subreddit_id, mention_count, sentiment_avg
                        )
                        SELECT
                            tech_name,
                            week_start,
                            subreddit_id,
                            mention_count,
                            sentiment_avg
                        FROM
                            temp_tech_trends
                        ORDER BY
                            week_start, tech_name, mention_count DESC
                    """)

            # Lấy số lượng xu hướng đã tạo
            cur.execute("SELECT COUNT(*) FROM reddit_data.tech_trends")
            trend_count = cur.fetchone()[0]

            cur.execute("DROP TABLE IF EXISTS temp_tech_trends")

            conn.commit()
            logger.info(f"Đã phân tích và cập nhật {trend_count} xu hướng công nghệ theo tuần")

            return trend_count
        except Exception as e:
            logger.error(f"Lỗi khi phân tích xu hướng công nghệ theo tuần: {str(e)}")
            if conn:
                conn.rollback()
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_tech_growth(self, period_weeks=4):
        """
            Phân tích tăng trưởng của các công nghệ so với kỳ trước

            Args:
                period_weeks (int): Số tuần để so sánh

            Returns:
                pandas.DataFrame: Kết quả phân tích tăng trưởng
        """
        conn = None
        try:
            conn = self.get_db_connection()

            cache_key = f"tech_growth_{period_weeks}"

            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(hours=6):
                    logger.info(f"Sử dụng kết quả phân tích tăng trưởng từ cache (period_weeks={period_weeks})")
                    return self.cache[cache_key]['data']

            query = f"""
                WITH current_period AS (
                    SELECT
                        tech_name,
                        SUM(mention_count) as current_mentions
                    FROM
                        reddit_data.tech_trends
                    WHERE
                        week_start >= (SELECT MAX(week_start) FROM reddit_data.tech_trends) - INTERVAL '{period_weeks} weeks'
                    GROUP BY
                        tech_name
                ),
                previous_period AS (
                    SELECT
                        tech_name,
                        SUM(mention_count) as previous_mentions
                    FROM
                        reddit_data.tech_trends
                    WHERE
                        week_start >= (SELECT MAX(week_start) FROM reddit_data.tech_trends) - INTERVAL '{period_weeks * 2} weeks'
                        AND week_start < (SELECT MAX(week_start) FROM reddit_data.tech_trends) - INTERVAL '{period_weeks} weeks'
                    GROUP BY
                        tech_name
                )
                SELECT
                    cp.tech_name,
                    cp.current_mentions,
                    COALESCE(pp.previous_mentions, 0) as previous_mentions,
                    CASE
                        WHEN COALESCE(pp.previous_mentions, 0) = 0 THEN NULL
                        ELSE (cp.current_mentions - pp.previous_mentions)::FLOAT / pp.previous_mentions * 100
                    END as growth_percent
                FROM
                    current_period cp
                    LEFT JOIN previous_period pp ON cp.tech_name = pp.tech_name
                WHERE
                    cp.current_mentions >= 5  -- Chỉ xem xét các công nghệ có ít nhất 5 lần đề cập
                ORDER BY
                    growth_percent DESC NULLS LAST
            """

            df = pd.read_sql_query(query, conn)

            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': df.copy(),
                    'timestamp': datetime.now()
                }

            logger.info(f"Đã phân tích tăng trưởng cho {len(df)} công nghệ")
            return df

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tăng trưởng công nghệ: {str(e)}")
            return pd.DataFrame()
        finally:
            if conn:
                self.return_db_connection(conn)

    def analyze_emerging_technologies(self, min_mentions=5, growth_threshold=50):
        """
            Phân tích các công nghệ mới nổi dựa trên tăng trưởng

            Args:
                min_mentions (int): Số lần đề cập tối thiểu
                growth_threshold (float): Ngưỡng tăng trưởng phần trăm

            Returns:
                pandas.DataFrame: Danh sách công nghệ mới nổi
        """
        conn = None
        try:
            cache_key = f"emerging_tech_{min_mentions}_{growth_threshold}"

            # Kiểm tra trong cache
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(hours=6):
                    logger.info(f"Sử dụng kết quả phân tích công nghệ mới nổi từ cache")
                    return self.cache[cache_key]['data']

            # Phân tích tăng trưởng
            growth_df = self.analyze_tech_growth()

            if growth_df.empty:
                logger.warning("Không có dữ liệu về tăng trưởng công nghệ")
                return pd.DataFrame()

            # Lọc các công nghệ mới nổi
            emerging_df = growth_df[
                (growth_df['current_mentions'] >= min_mentions) &
                (growth_df['growth_percent'] >= growth_threshold)
                ].copy()

            # Lấy thông tin về tình cảm (sentiment)
            if not emerging_df.empty:
                conn = self.get_db_connection()

                # Danh sách các công nghệ mới nổi
                tech_list = "', '".join(emerging_df['tech_name'].tolist())

                # Truy vấn điểm tình cảm trung bình
                query = f"""
                                SELECT
                                    tech_name,
                                    AVG(sentiment_avg) as avg_sentiment
                                FROM
                                    reddit_data.tech_trends
                                WHERE
                                    tech_name IN ('{tech_list}')
                                GROUP BY
                                    tech_name
                            """

                sentiment_df = pd.read_sql_query(query, conn)
                emerging_df = pd.merge(emerging_df, sentiment_df, on='tech_name', how='left')

                # Lưu vào database
                self._save_emerging_tech_to_db(emerging_df, conn)

            # Lưu vào cache
            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': emerging_df.copy(),
                    'timestamp': datetime.now()
                }

            logger.info(f"Đã xác định {len(emerging_df)} công nghệ mới nổi")
            return emerging_df

        except Exception as e:
            logger.error(f"Lỗi khi phân tích công nghệ mới nổi: {str(e)}")
            return pd.DataFrame()

        finally:
            if conn:
                self.return_db_connection(conn)

    def _save_emerging_tech_to_db(self, emerging_df, conn):
        """
        Lưu dữ liệu về công nghệ mới nổi vào database

        Args:
            emerging_df (pandas.DataFrame): Dataframe công nghệ mới nổi
            conn: Kết nối database
        """
        if emerging_df.empty:
            return

        try:
            # Chuẩn bị dữ liệu để insert
            current_date = datetime.now().date()
            records = []

            for _, row in emerging_df.iterrows():  # Sửa từ iter_rows thành iterrows
                records.append((
                    row['tech_name'],
                    current_date,
                    current_date - timedelta(days=30),  # period_start là 30 ngày trước
                    int(row['current_mentions']),
                    float(row.get('avg_sentiment', 0)),
                    float(row['growth_percent']),
                    None  # correlation_score, sẽ cập nhật sau nếu cần
                ))

            # Insert vào bảng sentiment_popularity_correlation
            cursor = conn.cursor()
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO reddit_data.sentiment_popularity_correlation
                (tech_name, period_end, period_start, mention_count, sentiment_avg, upvote_avg, correlation_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tech_name, period_end) 
                DO UPDATE SET
                    period_start = EXCLUDED.period_start,
                    mention_count = EXCLUDED.mention_count,
                    sentiment_avg = EXCLUDED.sentiment_avg,
                    upvote_avg = EXCLUDED.upvote_avg,
                    correlation_score = EXCLUDED.correlation_score,
                    processed_date = CURRENT_TIMESTAMP
            """, records)

            conn.commit()
            logger.info(f"Đã lưu {len(records)} công nghệ mới nổi vào database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Lỗi khi lưu công nghệ mới nổi vào database: {str(e)}")

    def analyze_tech_correlation(self, min_mentions=10):
        """
            Phân tích mối tương quan giữa các công nghệ (thường xuất hiện cùng nhau)

            Args:
                min_mentions (int): Số lần đề cập tối thiểu

            Returns:
                pandas.DataFrame: Ma trận tương quan
        """
        conn = None
        try:
            # Cache key cho kết quả phân tích
            cache_key = f"tech_correlation_{min_mentions}"

            # Kiểm tra trong cache
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(
                        hours=12):
                    logger.info(f"Sử dụng kết quả phân tích tương quan từ cache")
                    return self.cache[cache_key]['data']

            # Lấy kết nối từ pool
            conn = self.get_db_connection()

            # Danh sách cách công nghệ thường được đề cập
            query_popular = f"""
                            SELECT tech_name, SUM(mention_count) as total_mentions
                            FROM reddit_data.tech_trends
                            GROUP BY tech_name
                            HAVING SUM(mention_count) >= {min_mentions}
                            ORDER BY total_mentions DESC
                        """

            popular_techs = pd.read_sql_query(query_popular, conn)

            if popular_techs.empty:
                logger.warning("Không có đủ dữ liệu để phân tích tương quan")
                return pd.DataFrame()

            tech_list = popular_techs['tech_name'].tolist()

            # DF lưu trữ ma trận đồng xuất hiện
            correlation_matrix = pd.DataFrame(0, index=tech_list, columns=tech_list)
            query_posts = """
                SELECT post_id, tech_mentioned
                FROM reddit_data.post_analysis
                WHERE tech_mentioned IS NOT NULL
            """

            posts_df = pd.read_sql_query(query_posts, conn)
            # Đếm số lần đồng xuất hiện
            for _, row in posts_df.iterrows():
                techs = row['tech_mentioned']
                if techs and isinstance(techs, list):
                    # Lọc các công nghệ trong danh sách phổ biến
                    techs_in_list = [tech for tech in techs if tech in tech_list]

                    # Cập nhật ma trận
                    for i in range(len(techs_in_list)):
                        for j in range(i, len(techs_in_list)):
                            tech_i = techs_in_list[i]
                            tech_j = techs_in_list[j]
                            correlation_matrix.loc[tech_i, tech_j] += 1
                            if i != j:  # Không phải đường chéo
                                correlation_matrix.loc[tech_j, tech_i] += 1

            # Tính toán hệ số Jaccard để chuẩn hóa
            for i in range(len(tech_list)):
                for j in range(i + 1, len(tech_list)):
                    tech_i = tech_list[i]
                    tech_j = tech_list[j]

                    # Hệ số Jaccard: |A∩B| / |A∪B|
                    intersection = correlation_matrix.loc[tech_i, tech_j]
                    union = (
                            popular_techs.loc[popular_techs['tech_name'] == tech_i, 'total_mentions'].iloc[0] +
                            popular_techs.loc[popular_techs['tech_name'] == tech_j, 'total_mentions'].iloc[0] -
                            intersection
                    )

                    if union > 0:
                        jaccard = intersection / union
                    else:
                        jaccard = 0

                    correlation_matrix.loc[tech_i, tech_j] = jaccard
                    correlation_matrix.loc[tech_j, tech_i] = jaccard

            # Đặt 1.0 cho đường chéo
            for tech in tech_list:
                correlation_matrix.loc[tech, tech] = 1.0

            # Lưu kết quả vào database
            self._save_tech_correlation_to_db(correlation_matrix, conn)

            # Lưu vào cache
            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': correlation_matrix.copy(),
                    'timestamp': datetime.now()
                }

            logger.info(f"Đã phân tích tương quan giữa {len(tech_list)} công nghệ")
            return correlation_matrix

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tương quan công nghệ: {str(e)}")
            return pd.DataFrame()

        finally:
            if conn:
                self.return_db_connection(conn)

    def _save_tech_correlation_to_db(self, correlation_matrix, conn):
        """
            Lưu ma trận tương quan công nghệ vào database

            Args:
                correlation_matrix (pandas.DataFrame): Ma trận tương quan
                conn: Kết nối database
        """
        try:
            cursor = conn.cursor()
            # Xóa dữ liệu cũ
            analyzed_date = datetime.now().date()
            cursor.execute("DELETE FROM reddit_data.tech_correlation WHERE analyzed_date = %s", (analyzed_date,))

            # Chuẩn bị dữ liệu để insert
            tech_list = correlation_matrix.index.tolist()
            records = []
            for i in range(len(tech_list)):
                for j in range(i + 1, len(tech_list)):  # Chỉ lưu nửa trên của ma trận
                    tech_i = tech_list[i]
                    tech_j = tech_list[j]
                    correlation_score = correlation_matrix.loc[tech_i, tech_j]

                    # Chỉ lưu những cặp có tương quan đáng kể
                    if correlation_score > 0.1:
                        records.append((
                            tech_i,
                            tech_j,
                            correlation_score,
                            analyzed_date
                        ))

            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO reddit_data.tech_correlation
                (tech_name_1, tech_name_2, correlation_score, analyzed_date)
                VALUES (%s, %s, %s, %s)
            """, records)

            conn.commit()
            logger.info(f"Đã lưu {len(records)} cặp tương quan công nghệ vào database")
        except Exception as e:
            conn.rollback()
            logger.error(f"Lỗi khi lưu tương quan công nghệ vào database: {str(e)}")

    def analyze_skill_demand_trends(self):
        """
            Phân tích xu hướng nhu cầu kỹ năng theo thời gian

            Returns:
                pandas.DataFrame: Xu hướng nhu cầu kỹ năng
        """
        conn = None
        try:
            cache_key = "skill_demand_trends"

            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(hours=24):
                    logger.info("Sử dụng kết quả phân tích nhu cầu kỹ năng từ cache")
                    return self.cache[cache_key]['data']

            conn = self.get_db_connection()

            query = """
                SELECT
                    DATE_TRUNC('month', p.created_date) as month,
                    unnest(pa.skills_mentioned) as skill,
                    COUNT(*) as mention_count
                FROM
                    reddit_data.post_analysis pa
                    JOIN reddit_data.posts p ON pa.post_id = p.post_id
                WHERE
                    pa.skills_mentioned IS NOT NULL
                    AND p.created_date IS NOT NULL
                    AND (LOWER(p.title) LIKE '%hiring%' 
                        OR LOWER(p.title) LIKE '%job%'
                        OR LOWER(p.title) LIKE '%career%'
                        OR LOWER(p.title) LIKE '%looking for%'
                        OR LOWER(p.title) LIKE '%position%')
                GROUP BY
                    month, skill
                ORDER BY
                    month, mention_count DESC
            """

            df = pd.read_sql_query(query, conn)
            if df.empty:
                logger.warning("Không có dữ liệu về nhu cầu kỹ năng")
                return pd.DataFrame()

            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': df.copy(),
                    'timestamp': datetime.now()
                }

                logger.info(f"Đã phân tích xu hướng nhu cầu kỹ năng qua {df['month'].nunique()} tháng")
                return df

        except Exception as e:
            logger.error(f"Lỗi khi phân tích xu hướng nhu cầu kỹ năng: {str(e)}")
            return pd.DataFrame()

        finally:
            if conn:
                self.return_db_connection(conn)

    def analyze_subreddit_trends(self, min_mentions=3):
        """
            Phân tích xu hướng công nghệ theo subreddit

            Args:
                min_mentions (int): Số lần đề cập tối thiểu

            Returns:
                pandas.DataFrame: Kết quả phân tích xu hướng theo subreddit
        """
        conn = None
        cur = None
        try:
            cache_key = f"subreddit_trends_{min_mentions}"
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(hours=12):
                    logger.info("Sử dụng kết quả phân tích xu hướng subreddit từ cache")
                    return self.cache[cache_key]['data']

            conn = self.get_db_connection()
            cur = conn.cursor()

            cur.execute("TRUNCATE TABLE reddit_data.subreddit_tech_trends RESTART IDENTITY")

            # Phân tích xu hướng theo subreddit và lưu vào database
            query = f"""
                WITH subreddit_tech AS (
                    SELECT
                        s.subreddit_id,
                        s.name as subreddit_name,
                        unnest(pa.tech_mentioned) as tech_name,
                        DATE_TRUNC('week', p.created_date) as week_start,
                        DATE_TRUNC('month', p.created_date) as month_start,
                        COUNT(*) as mention_count,
                        AVG(pa.sentiment_score) as sentiment_avg
                    FROM
                        reddit_data.post_analysis pa
                        JOIN reddit_data.posts p ON pa.post_id = p.post_id
                        JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                    WHERE
                        pa.tech_mentioned IS NOT NULL
                    GROUP BY
                        s.subreddit_id, s.name, tech_name, week_start, month_start
                    HAVING
                        COUNT(*) >= {min_mentions}
                )
                INSERT INTO reddit_data.subreddit_tech_trends (
                    subreddit_id, tech_name, mention_count, sentiment_avg, 
                    week_start, month_start
                )
                SELECT
                    subreddit_id,
                    tech_name,
                    mention_count,
                    sentiment_avg,
                    week_start,
                    month_start
                FROM
                    subreddit_tech AS st
                ORDER BY
                    subreddit_id, month_start, mention_count DESC
            """

            cur.execute(query)

            cur.execute("SELECT COUNT(*) FROM reddit_data.subreddit_tech_trends")
            trend_count = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Đã phân tích và lưu {trend_count} xu hướng công nghệ theo subreddit")

            query_result = """
                SELECT
                    s.name as subreddit_name,
                    st.tech_name,
                    st.mention_count,
                    st.sentiment_avg,
                    st.week_start,
                    st.month_start
                FROM
                    reddit_data.subreddit_tech_trends st
                    JOIN reddit_data.subreddits s ON st.subreddit_id = s.subreddit_id
                ORDER BY
                    subreddit_name, month_start, mention_count DESC
            """
            df = pd.read_sql_query(query_result, conn)

            # Lưu vào cache
            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': df.copy(),
                    'timestamp': datetime.now()
                }

            return df

        except Exception as e:
            logger.error(f"Lỗi khi phân tích xu hướng theo subreddit: {str(e)}")
            if conn and cur:
                conn.rollback()
            return pd.DataFrame()

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_sentiment_popularity_correlation(self, min_mentions=10):
        """
            Phân tích mối tương quan giữa sentiment và mức độ phổ biến của công nghệ

            Args:
                min_mentions (int): Số lần đề cập tối thiểu

            Returns:
                pandas.DataFrame: Kết quả phân tích tương quan
        """
        conn = None
        try:
            # Cache key cho kết quả phân tích
            cache_key = f"sentiment_popularity_{min_mentions}"

            # Kiểm tra trong cache
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(
                        hours=12):
                    logger.info("Sử dụng kết quả phân tích tương quan sentiment-popularity từ cache")
                    return self.cache[cache_key]['data']

            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            query = f"""
                WITH tech_stats AS (
                    SELECT
                        unnest(pa.tech_mentioned) as tech_name,
                        pa.sentiment_score,
                        p.score as post_score,
                        p.num_comments,
                        DATE_TRUNC('month', p.created_date) as month
                    FROM
                        reddit_data.post_analysis pa
                        JOIN reddit_data.posts p ON pa.post_id = p.post_id
                    WHERE
                        pa.tech_mentioned IS NOT NULL
                        AND pa.sentiment_score IS NOT NULL
                )
                SELECT
                    tech_name,
                    month,
                    AVG(sentiment_score) as avg_sentiment,
                    AVG(post_score) as avg_score,
                    AVG(num_comments) as avg_comments,
                    COUNT(*) as mention_count
                FROM
                    tech_stats
                GROUP BY
                    tech_name, month
                HAVING
                    COUNT(*) >= {min_mentions}
                ORDER BY
                    tech_name, month
            """

            df = pd.read_sql_query(query, conn)

            if df.empty:
                logger.warning("Không đủ dữ liệu để phân tích tương quan sentiment-popularity")
                return pd.DataFrame()

            # Tính toán hệ số tương quan cho từng công nghệ
            tech_corr = {}
            for tech in df['tech_name'].unique():
                tech_df = df[df['tech_name'] == tech].copy()
                if len(tech_df) < 2:
                    continue

                # Tính hệ số tương quan Pearson
                sentiment_score_corr = tech_df['avg_sentiment'].corr(tech_df['avg_score'])
                sentiment_comments_corr = tech_df['avg_sentiment'].corr(tech_df['avg_comments'])

                avg_corr = np.nanmean([sentiment_score_corr, sentiment_comments_corr])

                tech_corr[tech] = {
                    'sentiment_score_corr': sentiment_score_corr,
                    'sentiment_comments_corr': sentiment_comments_corr,
                    'avg_correlation': avg_corr,
                    'mention_count': tech_df['mention_count'].sum(),
                    'avg_sentiment': tech_df['avg_sentiment'].mean(),
                    'avg_score': tech_df['avg_score'].mean(),
                    'avg_comments': tech_df['avg_comments'].mean()
                }

            result_df = pd.DataFrame.from_dict(tech_corr, orient='index')
            result_df.reset_index(inplace=True)
            result_df.rename(columns={'index': 'tech_name'}, inplace=True)

            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': result_df.copy(),
                    'timestamp': datetime.now()
                }

            logger.info(f"Đã phân tích tương quan sentiment-popularity cho {len(result_df)} công nghệ")
            return result_df

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tương quan sentiment-popularity: {str(e)}")
            return pd.DataFrame()

        finally:
            if conn:
                self.return_db_connection(conn)

    def _save_sentiment_popularity_corr_to_db(self, result_df, conn):
        """
            Lưu kết quả phân tích tương quan sentiment-popularity vào database

            Args:
                result_df (pandas.DataFrame): DataFrame kết quả phân tích
                conn: Kết nối database
        """
        if result_df.empty:
            return
        try:
            cursor = conn.cursor()

            # Chuẩn bị dữ liệu để insert
            current_date = datetime.now().date()
            start_date = current_date - timedelta(days=90)  # 3 tháng trước
            records = []

            for _, row in result_df.iterrows():
                records.append((
                    row['tech_name'],
                    start_date,
                    current_date,
                    int(row['mention_count']),
                    float(row['avg_sentiment']),
                    float(row['avg_score']),
                    float(row['avg_comments']),
                    float(row['avg_correlation'])
                ))

            # Insert vào bảng sentiment_popularity_correlation
            psycopg2.extras.execute_batch(cursor, """
                    INSERT INTO reddit_data.sentiment_popularity_correlation
                    (tech_name, period_start, period_end, mention_count, sentiment_avg, upvote_avg, comment_count_avg, correlation_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tech_name, period_end) 
                    DO UPDATE SET
                        period_start = EXCLUDED.period_start,
                        mention_count = EXCLUDED.mention_count,
                        sentiment_avg = EXCLUDED.sentiment_avg,
                        upvote_avg = EXCLUDED.upvote_avg,
                        comment_count_avg = EXCLUDED.comment_count_avg,
                        correlation_score = EXCLUDED.correlation_score,
                        processed_date = CURRENT_TIMESTAMP
                """, records)

            conn.commit()
            logger.info(f"Đã lưu {len(records)} kết quả tương quan sentiment-popularity vào database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Lỗi khi lưu tương quan sentiment-popularity vào database: {str(e)}")

    def analyze_tech_trends_by_time(self, time_unit='week', min_mentions=5):
        """
            Phân tích xu hướng công nghệ theo các đơn vị thời gian khác nhau

            Args:
                time_unit (str): Đơn vị thời gian ('day', 'week', 'month', 'quarter')
                min_mentions (int): Số lần đề cập tối thiểu

            Returns:
                pandas.DataFrame: Kết quả phân tích theo thời gian
        """

        conn = None
        try:
            # Kiểm tra đơn vị thời gian hợp lệ
            valid_units = ['day', 'week', 'month', 'quarter']
            if time_unit not in valid_units:
                logger.warning(f"Đơn vị thời gian không hợp lệ: {time_unit}. Sử dụng 'week' thay thế.")
                time_unit = 'week'

            cache_key = f"tech_trends_{time_unit}_{min_mentions}"
            # Kiểm tra trong cache
            with self.cache_lock:
                if cache_key in self.cache and self.cache[cache_key]['timestamp'] > datetime.now() - timedelta(hours=12):
                    logger.info(f"Sử dụng kết quả phân tích xu hướng theo {time_unit} từ cache")
                    return self.cache[cache_key]['data']

            conn = self.get_db_connection()

            query = f"""
                SELECT
                    unnest(pa.tech_mentioned) as tech_name,
                    DATE_TRUNC('{time_unit}', p.created_date) as time_period,
                    COUNT(*) as mention_count,
                    AVG(pa.sentiment_score) as avg_sentiment
                FROM
                    reddit_data.post_analysis pa
                    JOIN reddit_data.posts p ON pa.post_id = p.post_id
                WHERE
                    pa.tech_mentioned IS NOT NULL
                GROUP BY
                    tech_name, time_period
                HAVING
                    COUNT(*) >= {min_mentions}
                ORDER BY
                    time_period, mention_count DESC
            """
            df = pd.read_sql_query(query, conn)
            if df.empty:
                logger.warning(f"Không có dữ liệu xu hướng theo {time_unit}")
                return pd.DataFrame()

            with self.cache_lock:
                self.cache[cache_key] = {
                    'data': df.copy(),
                    'timestamp': datetime.now()
                }

            logger.info(f"Đã phân tích xu hướng theo {time_unit} cho {df['tech_name'].nunique()} công nghệ")
            return df
        except Exception as e:
            logger.error(f"Lỗi khi phân tích xu hướng theo {time_unit}: {str(e)}")
            return pd.DataFrame()

        finally:
            if conn:
                self.return_db_connection(conn)

    def run_all_analyses(self, parallel=True, max_workers=4):
        """
        Chạy tất cả các phân tích và lưu kết quả vào database

        Args:
            parallel (bool): Có chạy song song hay không
            max_workers (int): Số lượng thread tối đa khi chạy song song

        Returns:
            dict: Kết quả các phân tích đã chạy
        """
        start_time = time.time()
        results = {}

        # Danh sách các phân tích cần chạy
        analyses = [
            ('weekly_tech_trends', self.analyze_weekly_tech_trends, {}),
            ('tech_growth', self.analyze_tech_growth, {}),
            ('emerging_tech', self.analyze_emerging_technologies, {}),
            ('tech_correlation', self.analyze_tech_correlation, {}),
            ('skill_demand', self.analyze_skill_demand_trends, {}),
            ('subreddit_trends', self.analyze_subreddit_trends, {}),
            ('sentiment_popularity', self.analyze_sentiment_popularity_correlation, {}),
            ('tech_trends_weekly', self.analyze_tech_trends_by_time, {'time_unit': 'week'}),
            ('tech_trends_monthly', self.analyze_tech_trends_by_time, {'time_unit': 'month'})
        ]

        if parallel:
            logger.info(f"Chạy tất cả phân tích song song với {max_workers} threads")

            def run_analysis(name, func, kwargs):
                try:
                    result = func(**kwargs)
                    return name, result
                except Exception as e:
                    logger.error(f"Lỗi khi chạy phân tích {name}: {str(e)}")
                    return name, None

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(run_analysis, name, func, kwargs) for name, func, kwargs in analyses]

                for future in futures:
                    try:
                        name, result = future.result()
                        results[name] = result
                    except Exception as e:
                        logger.error(f"Lỗi khi lấy kết quả phân tích: {str(e)}")
        else:
            logger.info("Chạy tất cả phân tích tuần tự")
            for name, func, kwargs in analyses:
                try:
                    results[name] = func(**kwargs)
                    logger.info(f"Đã hoàn thành phân tích: {name}")
                except Exception as e:
                    logger.error(f"Lỗi khi chạy phân tích {name}: {str(e)}")
                    results[name] = None

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Đã hoàn thành tất cả phân tích trong {duration:.2f} giây")

        return results

    def close(self):
        """Đóng connection pool"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info("Đã đóng tất cả kết nối trong pool")



