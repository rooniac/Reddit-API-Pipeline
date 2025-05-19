# Reddit Data Engineering Pipeline

![version](https://img.shields.io/badge/version-1.0.0-blue)
![python](https://img.shields.io/badge/python-3.9.7-green)

## 📋 Tổng quan

Reddit Data Engineering Pipeline là hệ thống phân tích dữ liệu từ các cộng đồng Reddit về lĩnh vực Data Engineering. Dự án này thu thập bài viết và bình luận từ các subreddit liên quan đến Data Engineering, xử lý và phân tích để tìm ra xu hướng công nghệ, phân tích cảm xúc, và các insights về nhu cầu kỹ năng trong ngành.

Hệ thống được thiết kế theo kiến trúc pipeline hoàn chỉnh từ thu thập đến lưu trữ, phân tích và trực quan hóa dữ liệu. Sử dụng các công nghệ hiện đại như Kafka để xử lý dữ liệu theo thời gian thực, PostgreSQL để lưu trữ, và Dash/Plotly để trực quan hóa kết quả.

## 🔧 Công nghệ sử dụng

- **Python 3.9.7**: Ngôn ngữ lập trình chính
- **Redis API (PRAW)**: Thu thập dữ liệu từ Reddit
- **Apache Kafka**: Xử lý dữ liệu theo thời gian thực
- **PostgreSQL**: Lưu trữ dữ liệu
- **NLP (NLTK, VADER)**: Phân tích văn bản và cảm xúc
- **Dash/Plotly**: Trực quan hóa dữ liệu
- **Docker**: Containerization cho Kafka và Zookeeper

## 🏗️ Kiến trúc hệ thống

Hệ thống được thiết kế theo kiến trúc modular với các thành phần sau:

1. **Module thu thập dữ liệu**: Sử dụng Reddit API để thu thập bài viết và bình luận từ các subreddits
2. **Module xử lý dữ liệu**: Kafka Consumer xử lý và lưu trữ dữ liệu vào PostgreSQL
3. **Module phân tích dữ liệu**: 
   - Phân tích từ khóa và chủ đề
   - Phân tích cảm xúc
   - Phân tích xu hướng theo thời gian
4. **Module trực quan hóa**: Dashboard hiển thị các insights và kết quả phân tích


## 📁 Cấu trúc dự án

```bash
reddit_pipeline/
├── .env                       # Các biến môi trường (chứa thông tin xác thực API, kết nối DB)
├── .gitignore                 # Cấu hình Git ignore
├── README.md                  # Tài liệu dự án
├── requirements.txt           # Các thư viện cần thiết
├── docker-compose.yml         # Cấu hình Docker Compose cho Kafka và Zookeeper
│
├── config/                    # Thư mục chứa các file cấu hình
│   ├── technologies.json      # Danh sách công nghệ phân tích
│   ├── skills.json            # Danh sách kỹ năng
│   └── tech_sentiment.json    # Từ điển phân tích cảm xúc
│
├── data/                      # Thư mục chứa dữ liệu
│   ├── raw/                   # Dữ liệu thô
│   │   ├── posts/             # Bài viết Reddit
│   │   └── comments/          # Bình luận Reddit
│   ├── processed/             # Dữ liệu đã xử lý
│   └── output/                # Kết quả phân tích
│       ├── reports/           # Báo cáo phân tích
│       └── visualizations/    # Các hình ảnh trực quan hóa
│
├── logs/                      # Thư mục chứa file logs
│   ├── data_collection.log
│   ├── data_processing.log
│   ├── data_analysis.log
│   └── dashboard.log
│
├── src/                       # Mã nguồn chính của dự án
│   ├── data_collection/       # Module thu thập dữ liệu
│   │   └── reddit_collector.py
│   │
│   ├── data_processing/       # Module xử lý dữ liệu
│   │   └── kafka_consumer.py
│   │
│   ├── data_analysis/         # Module phân tích dữ liệu
│   │   ├── keyword_analyzer.py
│   │   ├── sentiment_analyzer.py
│   │   └── trend_analyzer.py  
│   │
│   ├── data_visualization/    # Module trực quan hóa
│   │   └── dashboard.py
│   │
│   └── utils/                 # Các tiện ích
│       ├── config.py          # Cấu hình dự án
│       └── logger.py          # Cấu hình logging
│    
│
└── scripts/                   # Các script chạy các tác vụ
    ├── setup_database.py      # Script thiết lập database
    ├── collect_reddit_data.py # Script thu thập dữ liệu Reddit
    ├── process_reddit_data.py # Script xử lý dữ liệu
    ├── analyze_keywords.py    # Script phân tích từ khóa
    ├── analyze_sentiment.py   # Script phân tích tình cảm
    ├── analyze_trends.py      # Script phân tích xu hướng
    └── run_dashboard.py       # Script chạy dashboard
```

## 🚀 Hướng dẫn cài đặt

### Yêu cầu

- Python 3.9.7
- Docker và Docker Compose
- PostgreSQL
- Tài khoản Reddit Developer

### Cài đặt (khuyến khích sử dụng pycharm IDE)

1. Clone repository
```bash
git clone https://github.com/rooniac/Reddit-API-Pipeline.git
cd reddit-data-engineering-pipeline
```

2. Tạo và kích hoạt môi trường ảo
```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate  # Windows
```

3. Cài đặt các thư viện cần thiết
```bash
pip install -r requirements.txt
```

4. Tạo file `.env` với các thông tin cấu hình cần thiết
```
# Reddit API Settings
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT="script:reddit_data_pipeline:v1.0 (by /u/your_username)"
REDDIT_USERNAME=your_username
REDDIT_PASSWORD=your_password

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_POSTS_TOPIC=reddit_posts
KAFKA_COMMENTS_TOPIC=reddit_comments

# PostgreSQL Settings
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=reddit_data
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

5. Khởi động Kafka và Zookeeper bằng Docker Compose
```bash
docker-compose up -d
```

6. Thiết lập database PostgreSQL
```bash
python scripts/setup_database.py
```

## 📊 Sử dụng

### Thu thập dữ liệu
```bash
python scripts/collect_reddit_data.py
```

### Xử lý dữ liệu
```bash
python scripts/process_reddit_data.py
```

### Phân tích dữ liệu
```bash
python scripts/analyze_keywords.py  # Phân tích từ khóa
python scripts/analyze_sentiment.py  # Phân tích tình cảm
python scripts/analyze_trends.py  # Phân tích xu hướng
```

### Trực quan hóa dữ liệu
```bash
python scripts/run_dashboard.py
```
Sau khi chạy, dashboard sẽ khả dụng tại http://localhost:8050

## 📈 Tính năng chính

- Thu thập và lưu trữ bài viết và bình luận từ các subreddit
- Phân tích và trích xuất từ khóa, chủ đề từ nội dung
- Phân tích cảm xúc đối với công nghệ và doanh nghiệp
- Phát hiện xu hướng công nghệ theo thời gian
- Xác định mối quan hệ giữa các công nghệ
- Phân tích nhu cầu kỹ năng trong lĩnh vực Data Engineering
- Dashboard trực quan hóa hiển thị các insights và kết quả phân tích

## 👨‍💻 Tác giả

- Tên của bạn - [GitHub](https://github.com/rooniac)
- Email: hungle03.work@gmail.com


