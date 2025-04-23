CREATE SCHEMA IF NOT EXISTS reddit_data;

-- Bảng chứa thông tin subreddit
CREATE TABLE IF NOT EXISTS reddit_data.subreddits(
    subreddit_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    subscribers INT,
    created_utc BIGINT,
    created_date TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng chứa thông tin các bài viết
CREATE TABLE IF NOT EXISTS reddit_data.posts (
    post_id VARCHAR(10) PRIMARY KEY,
    subreddit_id INT REFERENCES reddit_data.subreddits(subreddit_id),
    title TEXT NOT NULL,
    text TEXT,
    url TEXT,
    author VARCHAR(128),
    score INT,
    upvote_ratio FLOAT,
    num_comments INT,
    created_utc BIGINT,
    created_date TIMESTAMP,
    is_self BOOLEAN,
    is_video BOOLEAN,
    over_18 BOOLEAN,
    permalink TEXT,
    link_flair_text TEXT,
    collected_utc BIGINT,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng chứa thông tin bình luận
CREATE TABLE IF NOT EXISTS reddit_data.comments (
    comment_id VARCHAR(10) PRIMARY KEY,
    post_id VARCHAR(10) REFERENCES reddit_data.posts(post_id),
    parent_id VARCHAR(12),
    body TEXT,
    author VARCHAR(128),
    score INT,
    created_utc BIGINT,
    created_date TIMESTAMP,
    is_submitter BOOLEAN,
    collected_utc BIGINT,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng chứa dữ liệu đã xử lý NLP của bài viết
CREATE TABLE IF NOT EXISTS reddit_data.post_analysis (
    analysis_id SERIAL PRIMARY KEY,
    post_id VARCHAR(10) REFERENCES reddit_data.posts(post_id),
    sentiment_score FLOAT,         -- Điểm cảm xúc của bài viết (-1 đến 1)
    word_count INT,                -- Số từ trong bài viết
    unique_words INT,              -- Số từ duy nhất
    tech_mentioned TEXT[],         -- Các công nghệ được đề cập
    companies_mentioned TEXT[],    -- Các công ty được đề cập
    skills_mentioned TEXT[],       -- Các kỹ năng được đề cập
    topics TEXT[],                 -- Các chủ đề chính được phát hiện
    is_question BOOLEAN,           -- Có phải là câu hỏi?
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng chứa dữ liệu đã xử lý NLP của bình luận
CREATE TABLE IF NOT EXISTS reddit_data.comment_analysis (
    analysis_id SERIAL PRIMARY KEY,
    comment_id VARCHAR(10) REFERENCES reddit_data.comments(comment_id),
    sentiment_score FLOAT,         -- Điểm cảm xúc của bình luận
    word_count INT,                -- Số từ trong bình luận
    tech_mentioned TEXT[],         -- Các công nghệ được đề cập
    is_answer BOOLEAN,             -- Có phải là câu trả lời cho một câu hỏi?
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng theo dõi xu hướng theo thời gian
CREATE TABLE IF NOT EXISTS reddit_data.tech_trends (
    trend_id SERIAL PRIMARY KEY,
    tech_name VARCHAR(100),
    mention_count INT,
    week_start DATE,               -- Ngày bắt đầu tuần thống kê
    sentiment_avg FLOAT,           -- Điểm cảm xúc trung bình
    subreddit_id INT REFERENCES reddit_data.subreddits(subreddit_id),
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng theo dõi hoạt động của người dùng
CREATE TABLE IF NOT EXISTS reddit_data.user_activity (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(128) UNIQUE,
    post_count INT DEFAULT 0,
    comment_count INT DEFAULT 0,
    avg_post_score FLOAT DEFAULT 0,
    avg_comment_score FLOAT DEFAULT 0,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    active_subreddits TEXT[],
    tech_expertise TEXT[],         -- Các công nghệ mà người dùng có vẻ thành thạo
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng lưu trữ mối tương quan giữa các công nghệ
CREATE TABLE IF NOT EXISTS reddit_data.tech_correlation (
    correlation_id SERIAL PRIMARY KEY,
    tech_name_1 VARCHAR(100),
    tech_name_2 VARCHAR(100),
    correlation_score FLOAT,
    analyzed_date DATE,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng phân tích xu hướng theo subreddit
CREATE TABLE IF NOT EXISTS reddit_data.subreddit_tech_trends (
    trend_id SERIAL PRIMARY KEY,
    subreddit_id INT REFERENCES reddit_data.subreddits(subreddit_id),
    tech_name VARCHAR(100),
    mention_count INT,
    sentiment_avg FLOAT,
    week_start DATE,
    month_start DATE,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng phân tích mối tương quan giữa sentiment và độ phổ biến
CREATE TABLE IF NOT EXISTS reddit_data.sentiment_popularity_correlation (
    record_id SERIAL PRIMARY KEY,
    tech_name VARCHAR(100),
    period_start DATE,
    period_end DATE,
    mention_count INT,
    sentiment_avg FLOAT,
    upvote_avg FLOAT,
    comment_count_avg FLOAT,
    correlation_score FLOAT, -- Điểm tương quan giữa sentiment và popularity
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Thêm ràng buộc unique cho bảng sentiment_popularity_correlation (first time run code)
--ALTER TABLE reddit_data.sentiment_popularity_correlation
--ADD CONSTRAINT unique_tech_period UNIQUE (tech_name, period_end);

-- Tạo index để tối ưu truy vấn
CREATE INDEX IF NOT EXISTS idx_posts_created_date ON reddit_data.posts(created_date);
CREATE INDEX IF NOT EXISTS idx_comments_created_date ON reddit_data.comments(created_date);
CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON reddit_data.posts(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_user_activity_username ON reddit_data.user_activity(username);
CREATE INDEX IF NOT EXISTS idx_tech_trends_name ON reddit_data.tech_trends(tech_name);

CREATE INDEX IF NOT EXISTS idx_tech_correlation_tech_names ON reddit_data.tech_correlation(tech_name_1, tech_name_2);
CREATE INDEX IF NOT EXISTS idx_subreddit_tech_trends_subreddit ON reddit_data.subreddit_tech_trends(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_subreddit_tech_trends_tech ON reddit_data.subreddit_tech_trends(tech_name);
CREATE INDEX IF NOT EXISTS idx_subreddit_tech_trends_week ON reddit_data.subreddit_tech_trends(week_start);
CREATE INDEX IF NOT EXISTS idx_sentiment_popularity_tech ON reddit_data.sentiment_popularity_correlation(tech_name);
CREATE INDEX IF NOT EXISTS idx_sentiment_popularity_period ON reddit_data.sentiment_popularity_correlation(period_start, period_end);
