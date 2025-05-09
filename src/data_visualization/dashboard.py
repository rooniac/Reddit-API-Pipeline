import os
import sys
import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import networkx as nx
from datetime import datetime, timedelta
import flask
from flask_caching import Cache
import psycopg2
import psycopg2.extras
from wordcloud import WordCloud
import plotly.figure_factory as ff
import base64
from io import BytesIO
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import json
from dash.dependencies import Input, Output, State, ALL

# Thêm thư mục gốc vào sys.path để import các module khác
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.utils.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
)
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger(__name__, "logs/dashboard.log")


class RedditDashboard:
    """
    Class quản lý dashboard trực quan hóa dữ liệu từ Reddit
    """

    def __init__(self, debug=False):
        """
        Khởi tạo dashboard

        Args:
            debug (bool): Chế độ debug
        """
        self.debug = debug

        # Khởi tạo Dash app với Bootstrap components - sử dụng theme FLATLY
        self.app = dash.Dash(
            __name__,
            external_stylesheets=[dbc.themes.FLATLY],
            suppress_callback_exceptions=True,
            meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
        )

        # Thiết lập tiêu đề
        self.app.title = "Reddit Data Engineering Analytics Dashboard"

        # Khởi tạo cache
        self.cache = Cache(self.app.server, config={
            'CACHE_TYPE': 'filesystem',
            'CACHE_DIR': 'cache-directory',
            'CACHE_DEFAULT_TIMEOUT': 300  # 5 phút
        })

        # Khởi tạo kết nối database
        self.db_connection = self._create_db_connection()

        # Thiết lập layout
        self.app.layout = self._create_layout()

        # Đăng ký các callback
        self._register_callbacks()

    def _create_db_connection(self):
        """
        Tạo kết nối đến PostgreSQL

        Returns:
            connection: Kết nối PostgreSQL
        """
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Kết nối thành công đến PostgreSQL")
            return conn
        except Exception as e:
            logger.error(f"Lỗi khi kết nối đến PostgreSQL: {str(e)}")
            raise

    def _create_layout(self):
        """
        Tạo layout cho dashboard

        Returns:
            html.Div: Layout của dashboard
        """
        return html.Div([
            # Header
            html.Div([
                dbc.Container([
                    dbc.Row([
                        dbc.Col(html.I(className="fas fa-chart-bar fa-3x text-primary"), width=1),
                        dbc.Col([
                            html.H1("Reddit Data Engineering Analytics", className="text-primary font-weight-bold"),
                            html.P("Phân tích xu hướng công nghệ từ cộng đồng Data Engineering trên Reddit",
                                   className="lead")
                        ], width=11)
                    ], align="center", className="py-3")
                ], fluid=True)
            ], className="bg-light border-bottom shadow-sm mb-4"),

            # Container chính cho nội dung
            dbc.Container([
                # Filter chung - cải tiến thành một card riêng với shadow
                dbc.Card([
                    dbc.CardHeader([
                        html.H4("Bộ lọc", className="text-primary"),
                        html.P("Điều chỉnh các bộ lọc để xem dữ liệu phù hợp", className="text-muted mb-0")
                    ]),
                    dbc.CardBody([
                        dbc.Row([
                            # Filter thời gian
                            dbc.Col([
                                html.Label("Khoảng thời gian:", className="font-weight-bold"),
                                dcc.DatePickerRange(
                                    id="date-range",
                                    start_date=(datetime.now() - timedelta(days=90)).date(),
                                    end_date=datetime.now().date(),
                                    display_format="DD/MM/YYYY",
                                    className="w-100"
                                )
                            ], md=6),

                            # Filter subreddit
                            dbc.Col([
                                html.Label("Subreddit:", className="font-weight-bold"),
                                dcc.Dropdown(
                                    id="subreddit-filter",
                                    options=self._get_subreddit_options(),
                                    value="dataengineering",
                                    multi=True,
                                    className="w-100"
                                )
                            ], md=6),
                        ]),

                        dbc.Row([
                            dbc.Col([
                                html.Label("Số lượng hiển thị:", className="font-weight-bold mt-3"),
                                dcc.Slider(
                                    id="item-count-slider",
                                    min=5,
                                    max=30,
                                    step=5,
                                    value=10,
                                    marks={i: str(i) for i in range(5, 31, 5)},
                                    className="mt-2"
                                )
                            ], md=6),

                            dbc.Col([
                                html.Label("Số lần đề cập tối thiểu:", className="font-weight-bold mt-3"),
                                dcc.Slider(
                                    id="min-mentions-slider",
                                    min=1,
                                    max=20,
                                    step=1,
                                    value=5,
                                    marks={i: str(i) for i in [1, 5, 10, 15, 20]},
                                    className="mt-2"
                                )
                            ], md=6)
                        ])
                    ])
                ], className="mb-4 shadow-sm"),

                # Tabs chính - cải tiến với hiệu ứng shadow và padding
                dbc.Tabs([
                    # Tab Tổng quan
                    dbc.Tab([
                        # Row 1: KPI Cards với shadow và màu sắc
                        dbc.Row([
                            # Tổng số bài viết
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H2(id="total-posts-count", className="text-center text-primary"),
                                        html.P("Bài viết", className="text-center text-muted mb-0")
                                    ])
                                ], className="shadow-sm h-100 border-primary border-top")
                            ], md=3),

                            # Tổng số bình luận
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H2(id="total-comments-count", className="text-center text-success"),
                                        html.P("Bình luận", className="text-center text-muted mb-0")
                                    ])
                                ], className="shadow-sm h-100 border-success border-top")
                            ], md=3),

                            # Tổng số chủ đề
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H2(id="total-topics-count", className="text-center text-info"),
                                        html.P("Chủ đề", className="text-center text-muted mb-0")
                                    ])
                                ], className="shadow-sm h-100 border-info border-top")
                            ], md=3),

                            # Tổng số công nghệ
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardBody([
                                        html.H2(id="total-techs-count", className="text-center text-warning"),
                                        html.P("Công nghệ", className="text-center text-muted mb-0")
                                    ])
                                ], className="shadow-sm h-100 border-warning border-top")
                            ], md=3),
                        ], className="mb-4"),
                        # Row 2: Hoạt động theo thời gian và Top subreddits với cải tiến giao diện
                        dbc.Row([
                            # Hoạt động theo thời gian
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Hoạt động theo thời gian", className="mb-0"),
                                        html.Small("Số lượng bài viết theo thời gian", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-activity-trend",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="activity-trend-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '300px'}
                                                    )
                                                ], style={'height': '300px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="h-100 shadow-sm")
                            ], md=8),

                            # Top subreddits
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Top Subreddits", className="mb-0"),
                                        html.Small("Các cộng đồng có nhiều bài viết nhất", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-top-subreddits",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="top-subreddits-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '300px'}
                                                    )
                                                ], style={'height': '300px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="h-100 shadow-sm")
                            ], md=4),
                        ], className="mb-4"),

                        # Row 3: Phân bố tình cảm và Top công nghệ
                        dbc.Row([
                            # Phân bố tình cảm
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Phân bố tình cảm", className="mb-0"),
                                        html.Small("Tỷ lệ đánh giá tích cực/tiêu cực", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-sentiment-distribution",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="sentiment-distribution-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '300px'}
                                                    )
                                                ], style={'height': '300px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="h-100 shadow-sm")
                            ], md=6),

                            # Top công nghệ
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Top công nghệ được đề cập", className="mb-0"),
                                        html.Small("Các công nghệ phổ biến nhất", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-top-techs-overview",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="top-techs-overview-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '300px'}
                                                    )
                                                ], style={'height': '300px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="h-100 shadow-sm")
                            ], md=6),
                        ])
                    ], label="Tổng quan", tab_id="tab-overview", className="p-3"),
                    # Tab 1: Top chủ đề thảo luận và câu hỏi phổ biến - cải tiến giao diện
                    dbc.Tab([
                        dbc.Row([
                            # WordCloud cho top chủ đề
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Top chủ đề thảo luận", className="mb-0"),
                                        html.Small("Hiển thị bằng WordCloud", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-wordcloud",
                                            type="circle",
                                            children=[
                                                html.Div(
                                                    id="wordcloud-container",
                                                    style={'height': '350px', 'width': '100%', 'text-align': 'center'}
                                                )
                                            ]
                                        )
                                    ])
                                ], className="h-100 shadow-sm")
                            ], md=6),

                            # Biểu đồ top câu hỏi
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Câu hỏi phổ biến theo chủ đề", className="mb-0"),
                                        html.Small("Các chủ đề được hỏi nhiều nhất", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-questions",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="questions-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '350px'}
                                                    )
                                                ], style={'height': '350px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="h-100 shadow-sm")
                            ], md=6),
                        ], className="mb-4"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Danh sách chủ đề", className="mb-0"),
                                        html.Small("Nhấp vào một chủ đề để xem chi tiết", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        html.Div(
                                            id="topic-buttons-container",
                                            className="d-flex flex-wrap gap-2"
                                        )
                                    ])
                                ], className="mb-4 shadow-sm")
                            ], width=12),
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Chi tiết bài viết theo chủ đề", className="mb-0"),
                                        html.Small("Các bài viết liên quan đến chủ đề được chọn",
                                                   className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-topic-details",
                                            type="circle",
                                            children=[
                                                html.Div(id="topic-details-container")
                                            ]
                                        )
                                    ])
                                ], className="shadow-sm")
                            ])
                        ])
                    ], label="Top chủ đề thảo luận", className="p-3"),
                    # Tab 2: Tốc độ tăng trưởng công nghệ - cải tiến giao diện
                    dbc.Tab([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Tốc độ tăng trưởng công nghệ", className="mb-0"),
                                        html.Small("So sánh với kỳ trước", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-tech-growth",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="tech-growth-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '500px'}
                                                    )
                                                ], style={'height': '500px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0"),
                                    dbc.CardFooter([
                                        dbc.Row([
                                            dbc.Col([
                                                html.Label("Ngưỡng tăng trưởng (%):", className="font-weight-bold"),
                                                dcc.Slider(
                                                    id="growth-threshold-slider",
                                                    min=10,
                                                    max=100,
                                                    step=10,
                                                    value=20,
                                                    marks={i: str(i) for i in range(10, 101, 10)},
                                                    className="mt-2"
                                                )
                                            ], md=6),
                                            dbc.Col([
                                                html.Label("Số tuần so sánh:", className="font-weight-bold"),
                                                dcc.Slider(
                                                    id="growth-period-slider",
                                                    min=1,
                                                    max=12,
                                                    step=1,
                                                    value=4,
                                                    marks={i: str(i) for i in range(1, 13, 1)},
                                                    className="mt-2"
                                                )
                                            ], md=6)
                                        ])
                                    ])
                                ], className="shadow-sm")
                            ], md=12),
                        ], className="mb-4"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Top công nghệ được đề cập nhiều nhất", className="mb-0"),
                                        html.Small("Phân tích theo số lượng đề cập", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-top-techs",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="top-techs-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '500px'}
                                                    )
                                                ], style={'height': '500px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="shadow-sm")
                            ], md=12),
                        ])
                    ], label="Tốc độ tăng trưởng công nghệ", className="p-3"),
                    # Tab 3: Xu hướng công nghệ theo thời gian - cải tiến giao diện
                    dbc.Tab([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Xu hướng công nghệ theo thời gian", className="mb-0"),
                                        html.Small("Số lần đề cập theo khoảng thời gian", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dbc.Row([
                                            dbc.Col([
                                                html.Label("Chọn công nghệ:", className="font-weight-bold"),
                                                dcc.Dropdown(
                                                    id="tech-trend-dropdown",
                                                    options=self._get_tech_options(),
                                                    value=self._get_default_tech_values(),
                                                    multi=True,
                                                    className="w-100"
                                                )
                                            ], md=6),
                                            dbc.Col([
                                                html.Label("Đơn vị thời gian:", className="font-weight-bold"),
                                                dbc.ButtonGroup([
                                                    dbc.RadioItems(
                                                        id="time-unit-radio",
                                                        options=[
                                                            {"label": "Tuần", "value": "week"},
                                                            {"label": "Tháng", "value": "month"},
                                                            {"label": "Quý", "value": "quarter"}
                                                        ],
                                                        value="week",
                                                        inline=True,
                                                        className="btn-group",
                                                        inputClassName="btn-check",
                                                        labelClassName="btn btn-outline-primary",
                                                        labelCheckedClassName="active"
                                                    )
                                                ], className="w-100 mt-2")
                                            ], md=6)
                                        ], className="mb-3"),
                                        dcc.Loading(
                                            id="loading-tech-trends",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="tech-trends-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '450px'}
                                                    )
                                                ], style={'height': '450px', 'width': '100%'})
                                            ]
                                        )
                                    ])
                                ], className="shadow-sm")
                            ], md=12),
                        ], className="mb-4"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Phân bố xu hướng theo subreddit", className="mb-0"),
                                        html.Small("So sánh giữa các cộng đồng", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-subreddit-trends",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="subreddit-trends-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '450px'}
                                                    )
                                                ], style={'height': '450px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="shadow-sm")
                            ], md=12),
                        ])
                    ], label="Xu hướng công nghệ", className="p-3"),
                    # Tab 4: Mạng lưới tương quan giữa các công nghệ - cải tiến giao diện
                    dbc.Tab([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Mạng lưới tương quan công nghệ", className="mb-0"),
                                        html.Small("Các công nghệ thường xuất hiện cùng nhau", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-tech-network",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="tech-network-graph",
                                                        config={
                                                            'displayModeBar': 'hover',
                                                            'responsive': True,
                                                            'scrollZoom': True
                                                        },
                                                        style={'height': '500px'}
                                                    )
                                                ], style={'height': '500px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0"),
                                    dbc.CardFooter([
                                        html.Label("Ngưỡng tương quan:", className="font-weight-bold"),
                                        dcc.Slider(
                                            id="correlation-threshold-slider",
                                            min=0.1,
                                            max=0.9,
                                            step=0.1,
                                            value=0.1,
                                            marks={i / 10: str(i / 10) for i in range(1, 10)},
                                            className="mt-2"
                                        ),
                                        html.P("Di chuyển chuột để zoom và phóng to, nhấp vào nút để xem chi tiết",
                                               className="text-muted small mt-2 mb-0")
                                    ])
                                ], className="shadow-sm")
                            ], md=12),
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Chi tiết tương quan", className="mb-0"),
                                        html.Small("Các công nghệ liên quan với công nghệ được chọn",
                                                   className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-correlation-details",
                                            type="circle",
                                            children=[
                                                html.Div(
                                                    id="correlation-details-container",
                                                    className="p-2"
                                                )
                                            ]
                                        )
                                    ])
                                ], className="shadow-sm")
                            ], md=12),
                        ], className="mt-4")
                    ], label="Mạng lưới tương quan", className="p-3"),
                    # Tab 5: Bảng xếp hạng kỹ năng - cải tiến giao diện
                    dbc.Tab([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Kỹ năng được yêu cầu nhiều nhất", className="mb-0"),
                                        html.Small("Phân tích từ các bài viết tuyển dụng", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dbc.Row([
                                            dbc.Col([
                                                html.Label("Loại kỹ năng:", className="font-weight-bold"),
                                                dcc.Dropdown(
                                                    id="skill-type-dropdown",
                                                    options=[
                                                        {"label": "Kỹ thuật", "value": "technical"},
                                                        {"label": "Kỹ năng mềm", "value": "soft_skills"},
                                                        {"label": "Chứng chỉ", "value": "certifications"},
                                                        {"label": "Tất cả", "value": "all"}
                                                    ],
                                                    value="all",
                                                    className="w-100"
                                                )
                                            ], md=6),
                                            dbc.Col([
                                                html.Label("Cách hiển thị:", className="font-weight-bold"),
                                                dbc.ButtonGroup([
                                                    dbc.RadioItems(
                                                        id="skill-chart-type",
                                                        options=[
                                                            {"label": "Bar chart", "value": "bar"},
                                                            {"label": "Treemap", "value": "treemap"}
                                                        ],
                                                        value="bar",
                                                        inline=True,
                                                        className="btn-group",
                                                        inputClassName="btn-check",
                                                        labelClassName="btn btn-outline-primary",
                                                        labelCheckedClassName="active"
                                                    )
                                                ], className="w-100 mt-2")
                                            ], md=6)
                                        ], className="mb-3"),
                                        dcc.Loading(
                                            id="loading-skills-chart",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="skills-chart",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '450px'}
                                                    )
                                                ], style={'height': '450px', 'width': '100%'})
                                            ]
                                        )
                                    ])
                                ], className="shadow-sm")
                            ], md=12),
                        ], className="mb-4"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Xu hướng kỹ năng theo thời gian", className="mb-0"),
                                        html.Small("Phân tích thay đổi nhu cầu kỹ năng", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-skill-trends",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="skill-trends-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '450px'}
                                                    )
                                                ], style={'height': '450px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="shadow-sm")
                            ], md=12),
                        ])
                    ], label="Bảng xếp hạng kỹ năng", className="p-3"),

                    # Tab 6: Phân tích tình cảm về công nghệ
                    dbc.Tab([
                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Điểm tình cảm theo công nghệ", className="mb-0"),
                                        html.Small("Đánh giá tích cực/tiêu cực về công nghệ", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dbc.Row([
                                            dbc.Col([
                                                html.Label("Cách hiển thị:", className="font-weight-bold"),
                                                dbc.ButtonGroup([
                                                    dbc.RadioItems(
                                                        id="sentiment-chart-type",
                                                        options=[
                                                            {"label": "Heatmap", "value": "heatmap"},
                                                            {"label": "Radar", "value": "radar"},
                                                            {"label": "Bar", "value": "bar"}
                                                        ],
                                                        value="bar",
                                                        inline=True,
                                                        className="btn-group",
                                                        inputClassName="btn-check",
                                                        labelClassName="btn btn-outline-primary",
                                                        labelCheckedClassName="active"
                                                    )
                                                ], className="w-100 mt-2")
                                            ], md=6),
                                            dbc.Col([
                                                html.Label("Sắp xếp theo:", className="font-weight-bold"),
                                                dbc.ButtonGroup([
                                                    dbc.RadioItems(
                                                        id="sentiment-sort-by",
                                                        options=[
                                                            {"label": "Điểm tình cảm", "value": "sentiment"},
                                                            {"label": "Số lần đề cập", "value": "mentions"}
                                                        ],
                                                        value="sentiment",
                                                        inline=True,
                                                        className="btn-group",
                                                        inputClassName="btn-check",
                                                        labelClassName="btn btn-outline-primary",
                                                        labelCheckedClassName="active"
                                                    )
                                                ], className="w-100 mt-2")
                                            ], md=6)
                                        ], className="mb-3"),
                                        dcc.Loading(
                                            id="loading-sentiment-chart",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="sentiment-chart",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '450px'}
                                                    )
                                                ], style={'height': '450px', 'width': '100%'})
                                            ]
                                        )
                                    ])
                                ], className="shadow-sm")
                            ], md=12),
                        ], className="mb-4"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Xu hướng tình cảm theo thời gian", className="mb-0"),
                                        html.Small("Thay đổi đánh giá theo thời gian", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dbc.Row([
                                            dbc.Col([
                                                html.Label("Chọn công nghệ:", className="font-weight-bold"),
                                                dcc.Dropdown(
                                                    id="sentiment-tech-dropdown",
                                                    options=self._get_tech_options(),
                                                    value=self._get_default_tech_values(3),
                                                    multi=True,
                                                    className="w-100"
                                                )
                                            ], md=12)
                                        ], className="mb-3"),
                                        dcc.Loading(
                                            id="loading-sentiment-trend",
                                            type="circle",
                                            children=[
                                                html.Div([
                                                    dcc.Graph(
                                                        id="sentiment-trend-graph",
                                                        config={'displayModeBar': 'hover', 'responsive': True},
                                                        style={'height': '350px'}
                                                    )
                                                ], style={'height': '350px', 'width': '100%'})
                                            ]
                                        )
                                    ], className="p-3")  # Thêm padding cho card body
                                ], className="h-100 shadow-sm")  # Đảm bảo chiều cao 100%
                            ], md=6),
                            dbc.Col([
                                dbc.Card([
                                    dbc.CardHeader([
                                        html.H5("Ví dụ đánh giá", className="mb-0"),
                                        html.Small("Mẫu đánh giá tích cực và tiêu cực", className="text-muted")
                                    ]),
                                    dbc.CardBody([
                                        dcc.Loading(
                                            id="loading-sentiment-examples",
                                            type="circle",
                                            children=[
                                                html.Div(
                                                    id="sentiment-examples-container",
                                                    style={
                                                        'height': '420px',
                                                        'overflow-y': 'auto',
                                                        'padding': '10px'
                                                    }
                                                )
                                            ]
                                        )
                                    ], className="p-0")
                                ], className="h-100 shadow-sm")  # Đảm bảo chiều cao 100%
                            ], md=6),
                        ], className="mb-3", style={"min-height": "400px"})  # Đặt chiều cao tối thiểu cho hàng
                    ], label="Phân tích tình cảm", className="p-3"),
                ], id="main-tabs", active_tab="tab-overview", className="dbc"),

                # Footer với thông tin bổ sung
                html.Footer([
                    dbc.Container([
                        html.Hr(className="my-4"),
                        dbc.Row([
                            dbc.Col([
                                html.H6("Reddit Data Engineering Analytics", className="text-primary"),
                                html.P("Dự án phân tích dữ liệu Reddit - Data Engineering Pipeline",
                                       className="text-muted small")
                            ], md=6),
                            dbc.Col([
                                html.P([
                                    "© 2025 ",
                                    html.Span("Roonie", className="text-primary"),
                                    " - Sử dụng Python, Kafka, PostgreSQL và Dash"
                                ], className="text-muted small text-md-end")
                            ], md=6)
                        ])
                    ], fluid=True)
                ], className="mt-5 pt-3 bg-light")

            ], fluid=True)
        ])

    def _get_subreddit_options(self):
        """
            Lấy danh sách subreddit để hiển thị trong dropdown

            Returns:
                list: Danh sách options cho dropdown
        """
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT name FROM reddit_data.subreddits ORDER BY name")

            subreddits = cursor.fetchall()
            cursor.close()

            options = [{"label": sub[0], "value": sub[0]} for sub in subreddits]
            if not options:
                options = [{"label": "dataengineering", "value": "dataengineering"}]
            return options
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách subreddits: {str(e)}")
            # Trả về mặc định nếu có lỗi
            return [{"label": "dataengineering", "value": "dataengineering"}]

    def _get_tech_options(self):
        """
                Lấy danh sách công nghệ để hiển thị trong dropdown

                Returns:
                    list: Danh sách options cho dropdown
                """
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                        SELECT tech_name, SUM(mention_count) as total_mentions
                        FROM reddit_data.tech_trends
                        GROUP BY tech_name
                        ORDER BY total_mentions DESC
                        LIMIT 100
                    """)

            technologies = cursor.fetchall()
            cursor.close()

            # Format options cho dropdown
            options = [{"label": tech[0], "value": tech[0]} for tech in technologies]

            if not options:
                # Nếu không có dữ liệu, trả về một số mặc định
                options = [
                    {"label": "hadoop", "value": "hadoop"},
                    {"label": "spark", "value": "spark"},
                    {"label": "postgresql", "value": "postgresql"},
                    {"label": "airflow", "value": "airflow"},
                    {"label": "kafka", "value": "kafka"}
                ]

            return options
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách công nghệ: {str(e)}")
            # Trả về mặc định nếu có lỗi
            return [
                {"label": "hadoop", "value": "hadoop"},
                {"label": "spark", "value": "spark"},
                {"label": "postgresql", "value": "postgresql"},
                {"label": "airflow", "value": "airflow"},
                {"label": "kafka", "value": "kafka"}
            ]

    def _get_default_tech_values(self, count=5):
        """
            Lấy danh sách công nghệ mặc định

            Args:
                count (int): Số lượng công nghệ cần lấy

            Returns:
                list: Danh sách công nghệ mặc định
        """
        try:
            cursor = self.db_connection.cursor()
            cursor.execute("""
                SELECT tech_name
                FROM (
                    SELECT tech_name, SUM(mention_count) as total_mentions
                    FROM reddit_data.tech_trends
                    GROUP BY tech_name
                    ORDER BY total_mentions DESC
                    LIMIT %s
                ) t
            """, (count,))

            technologies = cursor.fetchall()
            cursor.close()

            return [tech[0] for tech in technologies] if technologies else ["hadoop", "spark", "postgresql", "airflow",
                                                                            "kafka"][:count]
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách công nghệ mặc định: {str(e)}")
            return ["hadoop", "spark", "postgresql", "airflow", "kafka"][:count]

    def _register_callbacks(self):
        """
        Đăng ký các callback cho dashboard
        """
        # Thêm các callbacks cho tab Tổng quan
        # Callback cho KPI Cards
        self.app.callback(
            [
                Output("total-posts-count", "children"),
                Output("total-comments-count", "children"),
                Output("total-topics-count", "children"),
                Output("total-techs-count", "children")
            ],
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value")
            ]
        )(self._update_overview_stats)

        # Callback cho biểu đồ hoạt động theo thời gian
        self.app.callback(
            Output("activity-trend-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value")
            ]
        )(self._update_activity_trend_graph)

        # Callback cho biểu đồ top subreddits
        self.app.callback(
            Output("top-subreddits-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date")
            ]
        )(self._update_top_subreddits_graph)

        # Callback cho biểu đồ phân bố tình cảm
        self.app.callback(
            Output("sentiment-distribution-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value")
            ]
        )(self._update_sentiment_distribution_graph)

        # Callback cho biểu đồ top công nghệ trong tab Tổng quan
        self.app.callback(
            Output("top-techs-overview-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value")
            ]
        )(self._update_top_techs_overview_graph)

        # Callback cho WordCloud chủ đề
        self.app.callback(
            Output("wordcloud-container", "children"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_wordcloud)

        # Callback cho danh sách chủ đề
        self.app.callback(
            Output("topic-buttons-container", "children"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_topic_buttons)

        # Callback cho biểu đồ câu hỏi
        self.app.callback(
            Output("questions-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_questions_graph)

        # Sử dụng pattern-matching callback cho tất cả các nút chủ đề
        self.app.callback(
            Output("topic-details-container", "children"),
            [
                Input({"type": "topic-button", "index": dash.ALL}, "n_clicks"),
                State({"type": "topic-button", "index": dash.ALL}, "id"),
                State("date-range", "start_date"),
                State("date-range", "end_date"),
                State("subreddit-filter", "value")
            ]
        )(self._update_topic_details)

        # Callback cho công nghệ mới nổi
        self.app.callback(
            Output("emerging-tech-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("min-mentions-slider", "value"),
                Input("growth-threshold-slider", "value")
            ]
        )(self._update_emerging_tech_graph)

        # Callback cho tốc độ tăng trưởng công nghệ (hợp nhất với công nghệ mới nổi)
        self.app.callback(
            Output("tech-growth-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("min-mentions-slider", "value"),
                Input("growth-threshold-slider", "value"),
                Input("growth-period-slider", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_tech_growth_graph)

        # Thêm callback cho top công nghệ được đề cập nhiều nhất
        self.app.callback(
            Output("top-techs-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_top_techs_graph)

        # Callback cho xu hướng công nghệ theo thời gian
        self.app.callback(
            Output("tech-trends-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("tech-trend-dropdown", "value"),
                Input("time-unit-radio", "value")
            ]
        )(self._update_tech_trends_graph)

        # Callback cho xu hướng theo subreddit
        self.app.callback(
            Output("subreddit-trends-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("subreddit-filter", "value"),
                Input("tech-trend-dropdown", "value")
            ]
        )(self._update_subreddit_trends_graph)

        # Callback cho mạng lưới tương quan
        self.app.callback(
            Output("tech-network-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("correlation-threshold-slider", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_tech_network_graph)

        # Callback cho chi tiết tương quan
        self.app.callback(
            Output("correlation-details-container", "children"),
            [
                Input("tech-network-graph", "clickData"),
                Input("date-range", "start_date"),
                Input("date-range", "end_date")
            ]
        )(self._update_correlation_details)

        # Callback cho biểu đồ kỹ năng
        self.app.callback(
            Output("skills-chart", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("skill-type-dropdown", "value"),
                Input("skill-chart-type", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_skills_chart)

        # Callback cho xu hướng kỹ năng
        self.app.callback(
            Output("skill-trends-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("skill-type-dropdown", "value"),
                Input("item-count-slider", "value")
            ]
        )(self._update_skill_trends_graph)

        # Callback cho biểu đồ tình cảm
        self.app.callback(
            Output("sentiment-chart", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("sentiment-chart-type", "value"),
                Input("sentiment-sort-by", "value"),
                Input("item-count-slider", "value"),
                Input("min-mentions-slider", "value")
            ]
        )(self._update_sentiment_chart)

        # Callback cho xu hướng tình cảm
        self.app.callback(
            Output("sentiment-trend-graph", "figure"),
            [
                Input("date-range", "start_date"),
                Input("date-range", "end_date"),
                Input("sentiment-tech-dropdown", "value"),
                Input("time-unit-radio", "value")
            ]
        )(self._update_sentiment_trend_graph)

        # Callback cho ví dụ tình cảm
        self.app.callback(
            Output("sentiment-examples-container", "children"),
            [
                Input("sentiment-chart", "clickData"),
                Input("date-range", "start_date"),
                Input("date-range", "end_date")
            ]
        )(self._update_sentiment_examples)

    def _update_overview_stats(self, start_date, end_date, subreddits):
        """Cập nhật các thống kê tổng quan"""
        try:
            subreddit_condition = ""
            params = [start_date, end_date]

            if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
                placeholders = ",".join(["%s"] * len(subreddits))
                subreddit_condition = f" AND s.name IN ({placeholders})"
                params.extend(subreddits)

            # Truy vấn số lượng bài viết
            query_posts = f"""
                SELECT COUNT(DISTINCT p.post_id) as count
                FROM reddit_data.posts p
                JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                WHERE p.created_date BETWEEN %s AND %s
                {subreddit_condition}
            """

            # Truy vấn số lượng bình luận
            query_comments = f"""
                SELECT COUNT(DISTINCT c.comment_id) as count
                FROM reddit_data.comments c
                JOIN reddit_data.posts p ON c.post_id = p.post_id
                JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                WHERE p.created_date BETWEEN %s AND %s
                {subreddit_condition}
            """

            # Truy vấn số lượng chủ đề
            query_topics = f"""
                SELECT COUNT(DISTINCT t.topic) as count
                FROM (
                    SELECT unnest(pa.topics) as topic
                    FROM reddit_data.post_analysis pa
                    JOIN reddit_data.posts p ON pa.post_id = p.post_id
                    JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                    WHERE p.created_date BETWEEN %s AND %s
                    {subreddit_condition}
                    AND pa.topics IS NOT NULL
                ) t
            """

            # Truy vấn số lượng công nghệ
            query_techs = f"""
                SELECT COUNT(DISTINCT tech_name) as count
                FROM reddit_data.tech_trends t
                JOIN reddit_data.subreddits s ON t.subreddit_id = s.subreddit_id
                WHERE t.week_start BETWEEN %s AND %s
                {subreddit_condition}
            """

            cur = self.db_connection.cursor()

            cur.execute(query_posts, params)
            total_posts = cur.fetchone()[0]

            cur.execute(query_comments, params)
            total_comments = cur.fetchone()[0]

            cur.execute(query_topics, params)
            total_topics = cur.fetchone()[0]

            cur.execute(query_techs, params)
            total_techs = cur.fetchone()[0]

            cur.close()

            # Format để hiển thị (thêm dấu phẩy cho số hàng nghìn)
            def format_number(num):
                return f"{num:,}".replace(",", " ")

            return format_number(total_posts), format_number(total_comments), format_number(
                total_topics), format_number(total_techs)
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật thống kê tổng quan: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return "N/A", "N/A", "N/A", "N/A"

    def _update_activity_trend_graph(self, start_date, end_date, subreddits):
        """
        Cập nhật biểu đồ hoạt động theo thời gian với cải tiến hiển thị
        """
        try:
            # Xác định đơn vị thời gian phù hợp dựa trên khoảng thời gian
            try:
                from datetime import datetime
                start_dt = datetime.strptime(start_date.split("T")[0], "%Y-%m-%d") if isinstance(start_date,
                                                                                                 str) and "T" in start_date else datetime.strptime(
                    start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date.split("T")[0], "%Y-%m-%d") if isinstance(end_date,
                                                                                             str) and "T" in end_date else datetime.strptime(
                    end_date, "%Y-%m-%d")
                days_diff = (end_dt - start_dt).days

                if days_diff <= 14:
                    time_unit = "day"
                    time_format = "%d/%m"
                elif days_diff <= 90:
                    time_unit = "week"
                    time_format = "%U/%Y"
                else:
                    time_unit = "month"
                    time_format = "%m/%Y"

            except Exception as e:
                logger.error(f"Lỗi khi tính khoảng thời gian: {str(e)}")
                time_unit = "week"
                time_format = "%U/%Y"

            # Tạo phần điều kiện lọc subreddit
            subreddit_condition = ""
            params = [start_date, end_date]

            if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
                placeholders = ",".join(["%s"] * len(subreddits))
                subreddit_condition = f" AND s.name IN ({placeholders})"
                params.extend(subreddits)

            # Truy vấn số lượng bài viết theo thời gian
            query = f"""
                SELECT 
                    DATE_TRUNC('{time_unit}', p.created_date)::date as time_period,
                    COUNT(p.post_id) as post_count
                FROM 
                    reddit_data.posts p
                    JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                WHERE 
                    p.created_date BETWEEN %s AND %s
                    {subreddit_condition}
                GROUP BY 
                    time_period
                ORDER BY 
                    time_period
            """

            # Thực hiện truy vấn
            df = pd.read_sql_query(query, self.db_connection, params=params)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu hoạt động',
                        'xaxis': {'title': 'Thời gian'},
                        'yaxis': {'title': 'Số lượng'},
                        'autosize': True
                    }
                }

            # Đảm bảo time_period là kiểu datetime
            df['time_period'] = pd.to_datetime(df['time_period'])

            # Format thời gian cho trục x
            df['time_label'] = df['time_period'].dt.strftime(time_format)

            # Tạo biểu đồ
            fig = px.line(
                df,
                x='time_period',
                y='post_count',
                title='Hoạt động theo thời gian',
                labels={
                    'post_count': 'Số lượng bài viết',
                    'time_period': 'Thời gian'
                },
                markers=True
            )

            # Cập nhật layout để đảm bảo hiển thị tốt
            fig.update_layout(
                autosize=True,
                margin=dict(l=10, r=10, t=40, b=40),
                xaxis=dict(
                    title='Thời gian',
                    tickmode='array',
                    tickvals=df['time_period'],
                    ticktext=df['time_label'],
                    tickangle=-45
                ),
                yaxis=dict(title='Số lượng bài viết')
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ hoạt động: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Thời gian'},
                    'yaxis': {'title': 'Số lượng'},
                    'autosize': True
                }
            }

    def _update_top_subreddits_graph(self, start_date, end_date):
        """
        Cập nhật biểu đồ top subreddits
        """
        try:
            # Truy vấn số lượng bài viết theo subreddit
            query = """
                SELECT 
                    s.name as subreddit_name,
                    COUNT(p.post_id) as post_count
                FROM 
                    reddit_data.posts p
                    JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                WHERE 
                    p.created_date BETWEEN %s AND %s
                GROUP BY 
                    s.name
                ORDER BY 
                    post_count DESC
                LIMIT 10
            """

            # Thực hiện truy vấn
            df = pd.read_sql_query(query, self.db_connection, params=[start_date, end_date])

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu subreddit',
                        'xaxis': {'title': 'Subreddit'},
                        'yaxis': {'title': 'Số lượng bài viết'},
                        'autosize': True
                    }
                }

            # Giới hạn số lượng subreddit hiển thị nếu có quá nhiều
            if len(df) > 5:
                df = df.head(5)

            # Tạo biểu đồ dọc
            fig = px.bar(
                df,
                x='subreddit_name',
                y='post_count',
                title='Top Subreddits',
                labels={
                    'post_count': 'Số lượng bài viết',
                    'subreddit_name': 'Subreddit'
                },
                color='post_count',
                color_continuous_scale='Viridis'
            )

            # # Thêm số liệu trên thanh
            # fig.update_traces(
            #     texttemplate='%{y}',
            #     textposition='outside'
            # )

            # Cập nhật layout
            fig.update_layout(
                height=300,
                autosize = True,
                margin=dict(l=10, r=10, t=30, b=10),
                coloraxis_showscale=False,
                xaxis=dict(title='Subreddit', tickangle=-45),
                yaxis=dict(title='Số lượng bài viết')
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ top subreddits: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Subreddit'},
                    'yaxis': {'title': 'Số lượng bài viết'}
                }
            }

    def _update_sentiment_distribution_graph(self, start_date, end_date, subreddits):
        """
        Cập nhật biểu đồ phân bố tình cảm
        """
        try:
            # Tạo phần điều kiện lọc subreddit
            subreddit_condition = ""
            params = [start_date, end_date]

            if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
                placeholders = ",".join(["%s"] * len(subreddits))
                subreddit_condition = f" AND s.name IN ({placeholders})"
                params.extend(subreddits)

            # Truy vấn phân bố tình cảm
            query = f"""
                WITH sentiment_ranges AS (
                    SELECT 
                        CASE
                            WHEN pa.sentiment_score < -0.5 THEN 'Rất tiêu cực'
                            WHEN pa.sentiment_score >= -0.5 AND pa.sentiment_score < -0.1 THEN 'Tiêu cực'
                            WHEN pa.sentiment_score >= -0.1 AND pa.sentiment_score < 0.1 THEN 'Trung tính'
                            WHEN pa.sentiment_score >= 0.1 AND pa.sentiment_score < 0.5 THEN 'Tích cực'
                            WHEN pa.sentiment_score >= 0.5 THEN 'Rất tích cực'
                        END as sentiment_range,
                        COUNT(*) as count
                    FROM 
                        reddit_data.post_analysis pa
                        JOIN reddit_data.posts p ON pa.post_id = p.post_id
                        JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                    WHERE 
                        p.created_date BETWEEN %s AND %s
                        AND pa.sentiment_score IS NOT NULL
                        {subreddit_condition}
                    GROUP BY 
                        sentiment_range
                )
                SELECT 
                    COALESCE(sentiment_range, 'Không có dữ liệu') as sentiment_range,
                    count
                FROM 
                    sentiment_ranges
                ORDER BY 
                    CASE sentiment_range
                        WHEN 'Rất tiêu cực' THEN 1
                        WHEN 'Tiêu cực' THEN 2
                        WHEN 'Trung tính' THEN 3
                        WHEN 'Tích cực' THEN 4
                        WHEN 'Rất tích cực' THEN 5
                        ELSE 6
                    END
            """

            # Thực hiện truy vấn
            df = pd.read_sql_query(query, self.db_connection, params=params)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu tình cảm trong khoảng thời gian được chọn',
                        'xaxis': {'title': 'Phân loại tình cảm'},
                        'yaxis': {'title': 'Số lượng bài viết'}
                    }
                }

            # Tạo bản đồ màu cho các phân loại tình cảm
            color_map = {
                'Rất tiêu cực': 'rgb(220, 50, 50)',
                'Tiêu cực': 'rgb(255, 150, 150)',
                'Trung tính': 'rgb(180, 180, 180)',
                'Tích cực': 'rgb(150, 200, 255)',
                'Rất tích cực': 'rgb(0, 100, 200)',
                'Không có dữ liệu': 'rgb(200, 200, 200)'
            }

            # Tạo biểu đồ
            fig = px.pie(
                df,
                names='sentiment_range',
                values='count',
                title='Phân bố tình cảm',
                color='sentiment_range',
                color_discrete_map=color_map
            )

            # Thêm nhãn phần trăm
            fig.update_traces(
                textinfo='percent+label',
                textposition='inside'
            )

            # Cập nhật layout
            fig.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.2,
                    xanchor="center",
                    x=0.5
                )
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ phân bố tình cảm: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Phân loại tình cảm'},
                    'yaxis': {'title': 'Số lượng bài viết'}
                }
            }

    def _update_top_techs_overview_graph(self, start_date, end_date, subreddits):
        """
        Cập nhật biểu đồ top công nghệ được đề cập trong tab Tổng quan với hiển thị cải tiến
        """
        try:
            # Tạo phần điều kiện lọc subreddit
            subreddit_condition = ""
            params = [start_date, end_date]

            if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
                subreddit_list = ", ".join([f"'{s}'" for s in subreddits])
                subreddit_condition = f"""
                    AND EXISTS (
                        SELECT 1 
                        FROM reddit_data.subreddits s 
                        WHERE t.subreddit_id = s.subreddit_id 
                        AND s.name IN ({subreddit_list})
                    )
                """

            # Truy vấn top công nghệ được đề cập
            query = f"""
                SELECT
                    tech_name,
                    SUM(mention_count) as total_mentions,
                    AVG(sentiment_avg) as avg_sentiment
                FROM
                    reddit_data.tech_trends t
                WHERE
                    week_start BETWEEN %s AND %s
                    {subreddit_condition}
                GROUP BY
                    tech_name
                ORDER BY
                    total_mentions DESC
                LIMIT 10
            """

            # Thực hiện truy vấn
            df = pd.read_sql_query(query, self.db_connection, params=params)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu công nghệ trong khoảng thời gian được chọn',
                        'xaxis': {'title': 'Số lần đề cập'},
                        'yaxis': {'title': 'Công nghệ'},
                        'autosize': True
                    }
                }

            # Sắp xếp theo số lần đề cập tăng dần để hiển thị đẹp hơn
            df = df.sort_values('total_mentions', ascending=True)

            # Tạo biểu đồ
            fig = px.bar(
                df,
                y='tech_name',
                x='total_mentions',
                title='Top công nghệ được đề cập',
                labels={
                    'total_mentions': 'Số lần đề cập',
                    'tech_name': 'Công nghệ',
                    'avg_sentiment': 'Điểm tình cảm trung bình'
                },
                orientation='h',
                color='avg_sentiment',
                color_continuous_scale='RdBu',
                color_continuous_midpoint=0,
                hover_data=['avg_sentiment'],
                text='total_mentions'
            )

            # Thêm text cho các bar
            fig.update_traces(
                texttemplate='%{x}',
                textposition='outside'
            )

            # Cập nhật layout
            fig.update_layout(
                autosize=True,
                margin=dict(l=10, r=10, t=40, b=10),
                coloraxis=dict(
                    colorbar=dict(
                        title='Điểm tình cảm',
                        tickvals=[-1, 0, 1],
                        ticktext=['Tiêu cực', 'Trung tính', 'Tích cực']
                    )
                ),
                xaxis=dict(title='Số lần đề cập'),
                yaxis=dict(title='', autorange="reversed")
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ top công nghệ: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Số lần đề cập'},
                    'yaxis': {'title': 'Công nghệ'},
                    'autosize': True
                }
            }

    def _format_date(self, date_str):
        """
        Chuyển đổi chuỗi ngày sang đối tượng datetime
        """
        try:
            if 'T' in date_str:
                return datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
            return datetime.strptime(date_str, '%Y-%m-%d')
        except:
            # Nếu không thể chuyển đổi, trả về ngày hiện tại
            return datetime.now()

    def _get_topics_data(self, start_date, end_date, subreddits):
        """
        Lấy dữ liệu chủ đề từ database

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            subreddits (list): Danh sách subreddit cần lọc

        Returns:
            pd.DataFrame: Dữ liệu chủ đề
        """
        # Tạo cache key
        subreddits_str = "_".join(sorted(subreddits)) if subreddits else "all"
        cache_key = f"topics_{start_date}_{end_date}_{subreddits_str}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            SELECT 
                unnest(pa.topics) as topic,
                COUNT(*) as count
            FROM 
                reddit_data.post_analysis pa
                JOIN reddit_data.posts p ON pa.post_id = p.post_id
                JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
            WHERE 
                p.created_date BETWEEN %s AND %s
                AND pa.topics IS NOT NULL
        """
        params = [start_date, end_date]

        if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
            placeholders = ",".join(["%s"] * len(subreddits))
            query += f" AND s.name IN ({placeholders})"
            params.extend(subreddits)

        query += " GROUP BY topic ORDER BY count DESC"

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu chủ đề: {str(e)}")
            return pd.DataFrame(columns=["topic", "count"])

    def _get_questions_data(self, start_date, end_date, subreddits):
        """
        Lấy dữ liệu câu hỏi từ database

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            subreddits (list): Danh sách subreddit cần lọc

        Returns:
            pd.DataFrame: Dữ liệu câu hỏi
        """
        # Tạo cache key
        subreddits_str = "_".join(sorted(subreddits)) if subreddits else "all"
        cache_key = f"questions_{start_date}_{end_date}_{subreddits_str}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            SELECT 
                unnest(pa.topics) as topic,
                COUNT(*) as count
            FROM 
                reddit_data.post_analysis pa
                JOIN reddit_data.posts p ON pa.post_id = p.post_id
                JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
            WHERE 
                p.created_date BETWEEN %s AND %s
                AND pa.is_question = true
                AND pa.topics IS NOT NULL
        """
        params = [start_date, end_date]

        if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
            placeholders = ",".join(["%s"] * len(subreddits))
            query += f" AND s.name IN ({placeholders})"
            params.extend(subreddits)

        query += " GROUP BY topic ORDER BY count DESC"

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu câu hỏi: {str(e)}")
            return pd.DataFrame(columns=["topic", "count"])

    def _check_tech_trends_table(self):
        """
        Kiểm tra cấu trúc và dữ liệu trong bảng tech_trends
        """
        try:
            # Kiểm tra cấu trúc bảng
            structure_query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'reddit_data' AND table_name = 'tech_trends'
            """
            structure_df = pd.read_sql(structure_query, self.db_connection)
            logger.info(f"Cấu trúc bảng tech_trends: {structure_df.to_dict('records')}")

            # Kiểm tra dữ liệu
            data_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT tech_name) as unique_techs,
                    MIN(week_start) as earliest_date,
                    MAX(week_start) as latest_date
                FROM reddit_data.tech_trends
            """
            data_df = pd.read_sql(data_query, self.db_connection)
            logger.info(f"Thông tin dữ liệu tech_trends: {data_df.to_dict('records')}")

            # Kiểm tra phân phối theo thời gian
            time_query = """
                SELECT 
                    DATE_TRUNC('month', week_start) as month,
                    COUNT(*) as record_count
                FROM reddit_data.tech_trends
                GROUP BY month
                ORDER BY month
            """
            time_df = pd.read_sql(time_query, self.db_connection)
            logger.info(f"Phân phối theo thời gian: {time_df.to_dict('records')}")

            # Trả về True nếu có dữ liệu
            return data_df['total_records'].iloc[0] > 0
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra bảng tech_trends: {str(e)}")
            return False

    def _get_tech_growth_data(self, start_date, end_date, min_mentions, period_weeks=4):
        """
        Lấy dữ liệu tăng trưởng công nghệ từ database

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            min_mentions (int): Số lần đề cập tối thiểu
            period_weeks (int): Số tuần để so sánh

        Returns:
            pd.DataFrame: Dữ liệu tăng trưởng
        """
        # Tạo cache key
        cache_key = f"tech_growth_{start_date}_{end_date}_{min_mentions}_{period_weeks}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        try:
            # Kiểm tra xem bảng có dữ liệu không
            has_data = self._check_tech_trends_table()
            if not has_data:
                logger.warning("Bảng tech_trends không có dữ liệu")
                return pd.DataFrame(columns=["tech_name", "current_mentions", "previous_mentions", "growth_percent"])

            # Sử dụng SQL thuần với mệnh đề WITH
            # Đảm bảo rằng start_date và end_date đúng định dạng
            try:
                # Cố gắng chuyển đổi định dạng ngày nếu cần
                from datetime import datetime
                if isinstance(start_date, str) and 'T' in start_date:
                    start_date = start_date.split('T')[0]
                if isinstance(end_date, str) and 'T' in end_date:
                    end_date = end_date.split('T')[0]
            except:
                pass

            # Xây dựng query để lấy dữ liệu hiện tại và kỳ trước
            query = """
                WITH date_params AS (
                    SELECT 
                        %s::date as start_date, 
                        %s::date as end_date
                ),
                current_period AS (
                    SELECT
                        t.tech_name,
                        SUM(t.mention_count) as current_mentions
                    FROM
                        reddit_data.tech_trends t,
                        date_params d
                    WHERE
                        t.week_start BETWEEN d.start_date AND d.end_date
                    GROUP BY
                        t.tech_name
                    HAVING
                        SUM(t.mention_count) >= %s
                ),
                dates AS (
                    SELECT
                        start_date,
                        end_date,
                        (start_date - (end_date - start_date))::date as prev_start_date,
                        start_date::date - interval '1 day' as prev_end_date
                    FROM date_params
                ),
                previous_period AS (
                    SELECT
                        t.tech_name,
                        SUM(t.mention_count) as previous_mentions
                    FROM
                        reddit_data.tech_trends t,
                        dates d
                    WHERE
                        t.week_start BETWEEN d.prev_start_date AND d.prev_end_date
                    GROUP BY
                        t.tech_name
                )
                SELECT
                    cp.tech_name,
                    cp.current_mentions,
                    COALESCE(pp.previous_mentions, 0) as previous_mentions,
                    CASE
                        WHEN COALESCE(pp.previous_mentions, 0) = 0 THEN NULL
                        ELSE ((cp.current_mentions - pp.previous_mentions)::FLOAT / pp.previous_mentions * 100)
                    END as growth_percent
                FROM
                    current_period cp
                    LEFT JOIN previous_period pp ON cp.tech_name = pp.tech_name
                ORDER BY
                    growth_percent DESC NULLS LAST
            """

            # Log parameters
            logger.info(
                f"Executing query with params: start_date={start_date}, end_date={end_date}, min_mentions={min_mentions}")

            # Thực hiện truy vấn
            df = pd.read_sql(query, self.db_connection, params=[start_date, end_date, min_mentions])
            logger.info(f"Query returned {len(df)} rows")

            # Log sample data
            if not df.empty:
                logger.info(f"Sample data: {df.head(3).to_dict('records')}")

            # Xử lý các giá trị NULL trong growth_percent
            df['growth_percent'] = df['growth_percent'].fillna(100)  # Giả sử tăng trưởng 100% cho các công nghệ mới

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu tăng trưởng công nghệ: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=["tech_name", "current_mentions", "previous_mentions", "growth_percent"])

    def _get_tech_trends_data(self, start_date, end_date, technologies, time_unit="week"):
        """
        Lấy dữ liệu xu hướng công nghệ theo thời gian

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            technologies (list): Danh sách công nghệ cần phân tích
            time_unit (str): Đơn vị thời gian (day, week, month, quarter)

        Returns:
            pd.DataFrame: Dữ liệu xu hướng
        """
        # Tạo cache key
        techs_str = "_".join(sorted(technologies)) if technologies else "all"
        cache_key = f"tech_trends_{start_date}_{end_date}_{techs_str}_{time_unit}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            WITH date_range AS (
                SELECT 
                    %s::date as start_date,
                    %s::date as end_date
            )
            SELECT
                tech_name,
                DATE_TRUNC(%s, week_start)::date as time_period,
                SUM(mention_count) as mentions,
                AVG(sentiment_avg) as avg_sentiment
            FROM
                reddit_data.tech_trends,
                date_range
            WHERE
                week_start BETWEEN date_range.start_date AND date_range.end_date
        """

        params = [start_date, end_date, time_unit]

        if technologies and len(technologies) > 0:
            placeholders = ",".join(["%s"] * len(technologies))
            query += f" AND tech_name IN ({placeholders})"
            params.extend(technologies)

        query += """
            GROUP BY
                tech_name, time_period
            ORDER BY
                time_period
        """

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu xu hướng công nghệ: {str(e)}")
            return pd.DataFrame(columns=["tech_name", "time_period", "mentions", "avg_sentiment"])

    def _get_subreddit_trends_data(self, start_date, end_date, subreddits, technologies):
        """
        Lấy dữ liệu xu hướng theo subreddit

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            subreddits (list): Danh sách subreddit cần lọc
            technologies (list): Danh sách công nghệ cần phân tích

        Returns:
            pd.DataFrame: Dữ liệu xu hướng theo subreddit
        """
        # Tạo cache key
        subreddits_str = "_".join(sorted(subreddits)) if subreddits else "all"
        techs_str = "_".join(sorted(technologies)) if technologies else "all"
        cache_key = f"subreddit_trends_{start_date}_{end_date}_{subreddits_str}_{techs_str}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            SELECT
                s.name as subreddit_name,
                tt.tech_name,
                SUM(tt.mention_count) as mentions,
                AVG(tt.sentiment_avg) as avg_sentiment
            FROM
                reddit_data.subreddit_tech_trends tt
                JOIN reddit_data.subreddits s ON tt.subreddit_id = s.subreddit_id
            WHERE
                tt.week_start BETWEEN %s AND %s
        """

        params = [start_date, end_date]

        # Filter by subreddit
        if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
            placeholders = ",".join(["%s"] * len(subreddits))
            query += f" AND s.name IN ({placeholders})"
            params.extend(subreddits)

        # Filter by technology
        if technologies and len(technologies) > 0:
            placeholders = ",".join(["%s"] * len(technologies))
            query += f" AND tt.tech_name IN ({placeholders})"
            params.extend(technologies)

        query += """
            GROUP BY
                s.name, tt.tech_name
            ORDER BY
                mentions DESC
        """

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu xu hướng theo subreddit: {str(e)}")
            return pd.DataFrame(columns=["subreddit_name", "tech_name", "mentions", "avg_sentiment"])

    def _get_tech_correlation_data(self, start_date, end_date, threshold=0.1):
        """
        Lấy dữ liệu tương quan công nghệ

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            threshold (float): Ngưỡng tương quan

        Returns:
            pd.DataFrame: Dữ liệu tương quan
        """
        # Tạo cache key
        cache_key = f"tech_correlation_{start_date}_{end_date}_{threshold}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        # query = """
        #     SELECT
        #         tech_name_1,
        #         tech_name_2,
        #         correlation_score
        #     FROM
        #         reddit_data.tech_correlation
        #     WHERE
        #         analyzed_date BETWEEN %s AND %s
        #         AND correlation_score >= %s
        #     ORDER BY
        #         correlation_score DESC
        # """
        query = """
                    SELECT
                        tech_name_1,
                        tech_name_2,
                        correlation_score
                    FROM
                        reddit_data.tech_correlation
                    WHERE
                        correlation_score >= %s
                    ORDER BY
                        correlation_score DESC
                """

        # params = [start_date, end_date, threshold]
        params = [threshold]

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu tương quan công nghệ: {str(e)}")
            return pd.DataFrame(columns=["tech_name_1", "tech_name_2", "correlation_score"])

    def _get_skills_data(self, start_date, end_date, skill_type="all"):
        """
        Lấy dữ liệu kỹ năng

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            skill_type (str): Loại kỹ năng (technical, soft_skills, certifications, all)

        Returns:
            pd.DataFrame: Dữ liệu kỹ năng
        """
        # Tạo cache key
        cache_key = f"skills_{start_date}_{end_date}_{skill_type}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            SELECT
                unnest(pa.skills_mentioned) as skill,
                COUNT(*) as count
            FROM
                reddit_data.post_analysis pa
                JOIN reddit_data.posts p ON pa.post_id = p.post_id
            WHERE
                p.created_date BETWEEN %s AND %s
                AND pa.skills_mentioned IS NOT NULL
        """

        params = [start_date, end_date]

        # Thêm điều kiện lọc theo loại kỹ năng nếu cần
        if skill_type != "all":
            # Giả định rằng chúng ta có bảng kỹ năng với trường category
            # Hoặc một cách khác để lọc kỹ năng theo loại
            pass

        query += """
            GROUP BY
                skill
            ORDER BY
                count DESC
        """

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu kỹ năng: {str(e)}")
            return pd.DataFrame(columns=["skill", "count"])

    def _get_sentiment_data(self, start_date, end_date, min_mentions=5):
        """
        Lấy dữ liệu tình cảm về công nghệ

        Args:
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc
            min_mentions (int): Số lần đề cập tối thiểu

        Returns:
            pd.DataFrame: Dữ liệu tình cảm
        """
        # Tạo cache key
        cache_key = f"sentiment_{start_date}_{end_date}_{min_mentions}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            SELECT
                tech_name,
                AVG(sentiment_avg) as avg_sentiment,
                SUM(mention_count) as mentions
            FROM
                reddit_data.tech_trends
            WHERE
                week_start BETWEEN %s AND %s
            GROUP BY
                tech_name
            HAVING
                SUM(mention_count) >= %s
            ORDER BY
                avg_sentiment DESC
        """

        params = [start_date, end_date, min_mentions]

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu tình cảm: {str(e)}")
            return pd.DataFrame(columns=["tech_name", "avg_sentiment", "mentions"])

    def _get_sentiment_examples(self, tech_name, start_date, end_date):
        """
        Lấy ví dụ đánh giá về công nghệ

        Args:
            tech_name (str): Tên công nghệ
            start_date (str): Ngày bắt đầu
            end_date (str): Ngày kết thúc

        Returns:
            pd.DataFrame: Ví dụ đánh giá
        """
        # Tạo cache key
        cache_key = f"sentiment_examples_{tech_name}_{start_date}_{end_date}"

        # Kiểm tra cache
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        query = """
            SELECT
                p.title,
                p.text,
                pa.sentiment_score,
                p.created_date,
                p.permalink  -- Thêm permalink
            FROM
                reddit_data.post_analysis pa
                JOIN reddit_data.posts p ON pa.post_id = p.post_id
            WHERE
                p.created_date BETWEEN %s AND %s
                AND pa.tech_mentioned @> ARRAY[%s]::text[]
            ORDER BY
                pa.sentiment_score DESC
            LIMIT 10
        """

        params = [start_date, end_date, tech_name]

        try:
            df = pd.read_sql(query, self.db_connection, params=params)

            # Lưu vào cache
            self.cache.set(cache_key, df, timeout=300)  # Cache trong 5 phút

            return df
        except Exception as e:
            logger.error(f"Lỗi khi lấy ví dụ đánh giá: {str(e)}")
            return pd.DataFrame(columns=["title", "text", "sentiment_score", "created_date", "permalink"])

    def _update_topic_buttons(self, start_date, end_date, subreddits, item_count):
        """
        Cập nhật danh sách nút chủ đề với giao diện cải tiến
        """
        try:
            # Lấy dữ liệu
            df = self._get_topics_data(start_date, end_date, subreddits)

            if df.empty:
                return html.Div("Không có chủ đề nào để hiển thị.", className="text-center text-muted")

            # Giới hạn số lượng chủ đề
            df = df.head(item_count)

            # Tạo danh sách các nút chủ đề có thể click với thiết kế đẹp hơn
            topic_buttons = []
            for i, row in df.iterrows():
                # Tạo màu sắc ngẫu nhiên nhưng nhất quán cho mỗi chủ đề
                color = "primary" if i % 3 == 0 else ("success" if i % 3 == 1 else "info")

                topic_buttons.append(
                    dbc.Button(
                        [
                            row['topic'],
                            html.Span(f" ({row['count']})", className="badge bg-light text-dark ms-1")
                        ],
                        id={"type": "topic-button", "index": row['topic']},
                        className=f'me-2 mb-2 btn-outline-{color}',
                        color=color,
                        outline=True,
                        n_clicks=0
                    )
                )

            return topic_buttons

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật nút chủ đề: {str(e)}")
            return html.Div(f"Lỗi khi tạo danh sách chủ đề: {str(e)}", className="text-danger")

    def _update_wordcloud(self, start_date, end_date, subreddits, item_count):
        """
        Cập nhật WordCloud chủ đề với cải tiến hiển thị
        """
        try:
            # Lấy dữ liệu
            df = self._get_topics_data(start_date, end_date, subreddits)

            if df.empty:
                return html.Div("Không có dữ liệu về chủ đề trong khoảng thời gian được chọn.",
                                style={'text-align': 'center', 'margin-top': '20px', 'color': '#6c757d'})

            # Giới hạn số lượng chủ đề
            df = df.head(item_count)

            # Tạo từ điển tần suất cho WordCloud
            topic_freq = {row['topic']: row['count'] for _, row in df.iterrows()}

            # Đặt matplotlib backend thành Agg (không dùng Tk)
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt

            # Tạo WordCloud với màu sắc đẹp hơn
            wordcloud = WordCloud(
                width=800,
                height=400,
                background_color='white',
                max_words=item_count,
                prefer_horizontal=1.0,
                colormap='viridis',
                contour_width=1,
                contour_color='#5a5a5a'
            ).generate_from_frequencies(topic_freq)

            # Tạo figure mới
            plt.figure(figsize=(10, 6))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')

            # Thêm tiêu đề đẹp hơn
            plt.title('Top chủ đề thảo luận', fontsize=16, pad=20, color='#2c3e50')

            # Chuyển đổi thành base64 để hiển thị trong Dash
            buffer = BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
            buffer.seek(0)
            img_str = base64.b64encode(buffer.read()).decode()

            # Đảm bảo đóng figure để tránh rò rỉ bộ nhớ
            plt.close('all')

            # Trả về hình ảnh với style để hiển thị tốt hơn
            return html.Img(
                src=f'data:image/png;base64,{img_str}',
                style={
                    'width': '100%',
                    'height': 'auto',
                    'max-height': '350px',
                    'object-fit': 'contain',
                    'border-radius': '8px',
                    'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
                }
            )

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật WordCloud: {str(e)}")
            return html.Div(f"Lỗi khi tạo WordCloud: {str(e)}", style={'color': 'red', 'text-align': 'center'})

    def _update_questions_graph(self, start_date, end_date, subreddits, item_count):
        """
        Cập nhật biểu đồ câu hỏi
        """
        try:
            # Lấy dữ liệu
            df = self._get_questions_data(start_date, end_date, subreddits)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu câu hỏi trong khoảng thời gian được chọn',
                        'xaxis': {'title': 'Số lượng'},
                        'yaxis': {'title': 'Chủ đề'}
                    }
                }

            # Giới hạn số lượng chủ đề
            df = df.head(item_count)

            # Tạo bar chart
            fig = px.bar(df, x='count', y='topic', orientation='h',
                         title='Câu hỏi phổ biến theo chủ đề',
                         labels={'count': 'Số lượng câu hỏi', 'topic': 'Chủ đề'},
                         color='count',
                         color_continuous_scale='Viridis')

            # Cập nhật layout
            fig.update_layout(
                height=400,
                margin=dict(l=10, r=10, t=30, b=10),
                coloraxis_showscale=False
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ câu hỏi: {str(e)}")
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Số lượng'},
                    'yaxis': {'title': 'Chủ đề'}
                }
            }

    def _update_topic_details(self, n_clicks_list, button_ids, start_date, end_date, subreddits):
        """
        Cập nhật chi tiết chủ đề khi một nút được nhấp
        """
        # Sử dụng dash.callback_context để xác định nút nào đã được click
        ctx = dash.callback_context

        if not ctx.triggered:
            return html.Div("Nhấp vào một chủ đề trong danh sách để xem chi tiết.", className="text-center p-3")

        # Kiểm tra xem có click nào chưa
        if not any(n_clicks_list) or all(n is None for n in n_clicks_list):
            return html.Div("Nhấp vào một chủ đề trong danh sách để xem chi tiết.", className="text-center p-3")

        # Lấy ID của nút được click gần đây nhất
        button_idx = None
        try:
            triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
            button_dict = json.loads(triggered_id)
            if button_dict.get('type') == 'topic-button':
                button_idx = button_dict.get('index')
        except:
            pass

        if button_idx is None:
            return html.Div("Không thể xác định chủ đề được chọn. Vui lòng thử lại.", className="text-center p-3")

        # Lấy chủ đề từ ID nút
        topic = button_idx

        try:
            # Truy vấn bài viết liên quan đến chủ đề
            query = """
                SELECT 
                    p.title,
                    p.text,
                    p.score,
                    p.num_comments,
                    p.created_date,
                    s.name as subreddit,
                    p.permalink
                FROM 
                    reddit_data.post_analysis pa
                    JOIN reddit_data.posts p ON pa.post_id = p.post_id
                    JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                WHERE 
                    p.created_date BETWEEN %s AND %s
                    AND %s = ANY(pa.topics)
            """
            params = [start_date, end_date, topic]

            if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
                placeholders = ",".join(["%s"] * len(subreddits))
                query += f" AND s.name IN ({placeholders})"
                params.extend(subreddits)

            query += " ORDER BY p.score DESC LIMIT 10"

            # Thực hiện truy vấn
            cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query, params)
            posts = cursor.fetchall()
            cursor.close()

            if not posts:
                return html.Div(f"Không tìm thấy bài viết nào về chủ đề '{topic}'.", className="text-center p-3")

            # Tạo danh sách bài viết
            post_list = []
            # for post in posts:
            #     post_card = dbc.Card([
            #         dbc.CardHeader(
            #             f"r/{post['subreddit']} - Score: {post['score']} - {post['created_date'].strftime('%d/%m/%Y')}"),
            #         dbc.CardBody([
            #             html.H5(post['title'], className="card-title"),
            #             html.P(post['text'][:200] + "..." if len(post['text']) > 200 else post['text'],
            #                    className="card-text"),
            #             html.Footer(f"Bình luận: {post['num_comments']}", className="text-muted")
            #         ])
            #     ], className="mb-3")
            #     post_list.append(post_card)
            for post in posts:
                # Tạo URL đầy đủ từ permalink
                reddit_url = f"https://www.reddit.com{post['permalink']}" if post['permalink'] else None

                # Tạo card với tiêu đề có thể click
                post_card = dbc.Card([
                    dbc.CardHeader(
                        f"r/{post['subreddit']} - Score: {post['score']} - {post['created_date'].strftime('%d/%m/%Y')}"),
                    dbc.CardBody([
                        # Nếu có URL, thêm link vào tiêu đề, nếu không chỉ hiển thị tiêu đề
                        html.H5(
                            html.A(post['title'], href=reddit_url, target="_blank") if reddit_url else post['title'],
                            className="card-title"
                        ),
                        html.P(post['text'][:200] + "..." if len(post['text']) > 200 else post['text'],
                               className="card-text"),
                        html.Footer([
                            html.Span(f"Bình luận: {post['num_comments']}", className="text-muted me-3"),
                            # Thêm nút xem bài viết
                            html.A("Xem trên Reddit", href=reddit_url, target="_blank",
                                   className="btn btn-sm btn-outline-primary") if reddit_url else None
                        ], className="d-flex justify-content-between align-items-center")
                    ])
                ], className="mb-3")
                post_list.append(post_card)

            return html.Div([
                html.H4(f"Bài viết về '{topic}'", className="mt-2 mb-3"),
                html.Div(post_list)
            ])

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật chi tiết chủ đề: {str(e)}")
            return html.Div(f"Lỗi khi tải chi tiết chủ đề: {str(e)}")

    def _update_emerging_tech_graph(self, start_date, end_date, min_mentions, growth_threshold):
        """
        Cập nhật biểu đồ công nghệ mới nổi
        """
        try:
            # Log input parameters
            logger.info(
                f"_update_emerging_tech_graph params: start_date={start_date}, end_date={end_date}, min_mentions={min_mentions}, growth_threshold={growth_threshold}")

            # Lấy dữ liệu tăng trưởng công nghệ
            df = self._get_tech_growth_data(start_date, end_date, min_mentions)

            # Ghi log số lượng bản ghi nhận được
            logger.info(f"_update_emerging_tech_graph - Nhận được {len(df)} bản ghi dữ liệu tăng trưởng")

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu công nghệ nào thỏa mãn điều kiện lọc.',
                        'xaxis': {'title': 'Phần trăm tăng trưởng'},
                        'yaxis': {'title': 'Công nghệ'}
                    }
                }

            # Lọc công nghệ mới nổi theo ngưỡng tăng trưởng
            emerging_df = df[df['growth_percent'] >= growth_threshold].copy()

            # Ghi log số lượng công nghệ mới nổi
            logger.info(
                f"_update_emerging_tech_graph - Tìm thấy {len(emerging_df)} công nghệ mới nổi với growth_threshold={growth_threshold}")

            if emerging_df.empty:
                # Kiểm tra xem có công nghệ nào tăng trưởng dương không
                positive_growth = df[df['growth_percent'] > 0]
                if not positive_growth.empty:
                    max_growth = positive_growth['growth_percent'].max()
                    return {
                        'data': [],
                        'layout': {
                            'title': f'Không có công nghệ nào có tốc độ tăng trưởng >= {growth_threshold}%. Thử giảm ngưỡng xuống dưới {max_growth:.1f}%',
                            'xaxis': {'title': 'Phần trăm tăng trưởng'},
                            'yaxis': {'title': 'Công nghệ'}
                        }
                    }
                else:
                    return {
                        'data': [],
                        'layout': {
                            'title': f'Không có công nghệ nào có tăng trưởng dương trong khoảng thời gian này.',
                            'xaxis': {'title': 'Phần trăm tăng trưởng'},
                            'yaxis': {'title': 'Công nghệ'}
                        }
                    }

            # Giới hạn số lượng hiển thị và sắp xếp
            if len(emerging_df) > 20:
                emerging_df = emerging_df.head(20)

            emerging_df = emerging_df.sort_values(by='growth_percent', ascending=True)

            # Tạo biểu đồ
            fig = px.bar(
                emerging_df,
                x='growth_percent',
                y='tech_name',
                orientation='h',
                title=f'Công nghệ mới nổi (Tăng trưởng >= {growth_threshold}%)',
                labels={
                    'growth_percent': 'Phần trăm tăng trưởng',
                    'tech_name': 'Công nghệ',
                    'current_mentions': 'Số lần đề cập hiện tại',
                    'previous_mentions': 'Số lần đề cập kỳ trước'
                },
                color='growth_percent',
                color_continuous_scale='Viridis',
                hover_data=['current_mentions', 'previous_mentions']
            )

            # Thêm số liệu trên thanh
            fig.update_traces(
                texttemplate='%{x:.1f}%',
                textposition='outside'
            )

            # Cập nhật layout
            fig.update_layout(
                height=500,
                margin=dict(l=10, r=10, t=30, b=10),
                coloraxis_showscale=False,
                xaxis=dict(title='Phần trăm tăng trưởng (%)')
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ công nghệ mới nổi: {str(e)}")
            # Ghi log chi tiết lỗi
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Phần trăm tăng trưởng'},
                    'yaxis': {'title': 'Công nghệ'}
                }
            }

    def _update_tech_growth_graph(self, start_date, end_date, min_mentions, growth_threshold, period_weeks, item_count):
        """
        Cập nhật biểu đồ tốc độ tăng trưởng công nghệ với cải tiến hiển thị
        """
        try:
            # Log input parameters
            logger.info(
                f"_update_tech_growth_graph params: start_date={start_date}, end_date={end_date}, min_mentions={min_mentions}, growth_threshold={growth_threshold}, period_weeks={period_weeks}, item_count={item_count}")

            # Lấy dữ liệu tăng trưởng công nghệ
            df = self._get_tech_growth_data(start_date, end_date, min_mentions, period_weeks)

            # Ghi log số lượng bản ghi nhận được
            logger.info(f"_update_tech_growth_graph - Nhận được {len(df)} bản ghi dữ liệu tăng trưởng")

            if df.empty:
                message = 'Không có dữ liệu công nghệ nào thỏa mãn điều kiện lọc.'
                return {
                    'data': [],
                    'layout': {
                        'title': message,
                        'xaxis': {'title': 'Công nghệ'},
                        'yaxis': {'title': 'Phần trăm tăng trưởng'},
                        'autosize': True
                    }
                }

            # Tạo thông tin kỳ so sánh
            try:
                from datetime import datetime, timedelta
                if isinstance(start_date, str) and 'T' in start_date:
                    start_date_dt = datetime.fromisoformat(start_date.split('T')[0])
                else:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')

                if isinstance(end_date, str) and 'T' in end_date:
                    end_date_dt = datetime.fromisoformat(end_date.split('T')[0])
                else:
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

                period_duration = (end_date_dt - start_date_dt).days
                prev_end_date_dt = start_date_dt - timedelta(days=1)
                prev_start_date_dt = prev_end_date_dt - timedelta(days=period_duration)

                current_period_str = f"{start_date_dt.strftime('%Y-%m-%d')} đến {end_date_dt.strftime('%Y-%m-%d')}"
                previous_period_str = f"{prev_start_date_dt.strftime('%Y-%m-%d')} đến {prev_end_date_dt.strftime('%Y-%m-%d')}"
                comparison_text = f"So sánh: {current_period_str} vs {previous_period_str}"
            except:
                comparison_text = f"Khoảng thời gian: {start_date} đến {end_date}"

            # Lọc theo ngưỡng tăng trưởng
            filtered_df = df[df['growth_percent'] >= growth_threshold].copy()

            if filtered_df.empty:
                # Kiểm tra xem có công nghệ nào tăng trưởng dương không
                positive_growth = df[df['growth_percent'] > 0]
                if not positive_growth.empty:
                    max_growth = positive_growth['growth_percent'].max()
                    return {
                        'data': [],
                        'layout': {
                            'title': f'Không có công nghệ nào có tốc độ tăng trưởng >= {growth_threshold}%. Thử giảm ngưỡng xuống dưới {max_growth:.1f}%',
                            'xaxis': {'title': 'Công nghệ'},
                            'yaxis': {'title': 'Phần trăm tăng trưởng'},
                            'autosize': True
                        }
                    }
                else:
                    return {
                        'data': [],
                        'layout': {
                            'title': f'Không có công nghệ nào có tăng trưởng dương trong khoảng thời gian này.',
                            'xaxis': {'title': 'Công nghệ'},
                            'yaxis': {'title': 'Phần trăm tăng trưởng'},
                            'autosize': True
                        }
                    }

            # Giới hạn số lượng công nghệ hiển thị
            if len(filtered_df) > item_count:
                filtered_df = filtered_df.head(item_count)

            # Sắp xếp theo tăng trưởng giảm dần
            filtered_df = filtered_df.sort_values(by='growth_percent', ascending=False)

            # Thêm tooltip rõ ràng hơn
            filtered_df['hover_text'] = filtered_df.apply(
                lambda row: f"<b>{row['tech_name']}</b><br>" +
                            f"Tăng trưởng: {row['growth_percent']:.1f}%<br>" +
                            f"Hiện tại: {int(row['current_mentions'])} lần đề cập<br>" +
                            f"Kỳ trước: {int(row['previous_mentions'])} lần đề cập",
                axis=1
            )

            # Tạo biểu đồ
            fig = px.bar(
                filtered_df,
                y='tech_name',
                x='growth_percent',
                title=f'Tốc độ tăng trưởng công nghệ (>= {growth_threshold}%)',
                labels={
                    'growth_percent': 'Phần trăm tăng trưởng (%)',
                    'tech_name': 'Công nghệ'
                },
                text='growth_percent',
                custom_data=['hover_text'],
                orientation='h',
                color='growth_percent',
                color_continuous_scale='Viridis'
            )

            # Thêm giá trị trên thanh
            fig.update_traces(
                texttemplate='%{x:.1f}%',
                textposition='outside',
                hovertemplate='%{customdata[0]}<extra></extra>'
            )

            # Cập nhật layout để cải thiện hiển thị
            fig.update_layout(
                autosize=True,
                margin=dict(l=10, r=50, t=40, b=40),
                coloraxis_showscale=False,
                xaxis=dict(
                    title=dict(
                        text='Phần trăm tăng trưởng (%)',
                        font=dict(size=14)
                    )
                ),
                yaxis=dict(
                    title=dict(
                        text='Công nghệ',
                        font=dict(size=14)
                    ),
                    autorange="reversed"
                ),
                title=dict(
                    text=f'Tốc độ tăng trưởng công nghệ (>= {growth_threshold}%)',
                    x=0.5,
                    xanchor='center'
                ),
                annotations=[
                    dict(
                        text=comparison_text,
                        x=0.5,
                        y=1.05,
                        xref="paper",
                        yref="paper",
                        showarrow=False,
                        font=dict(size=12, color="#666666")
                    )
                ]
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ tốc độ tăng trưởng: {str(e)}")
            # Ghi log chi tiết lỗi
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Phần trăm tăng trưởng'},
                    'yaxis': {'title': 'Công nghệ'},
                    'autosize': True
                }
            }

    def _update_top_techs_graph(self, start_date, end_date, subreddits, item_count):
        """
        Cập nhật biểu đồ top công nghệ được đề cập nhiều nhất
        """
        try:
            # Truy vấn để lấy top công nghệ được đề cập nhiều nhất
            query = """
                WITH date_params AS (
                    SELECT 
                        %s::date as start_date, 
                        %s::date as end_date
                )
                SELECT
                    tech_name,
                    SUM(mention_count) as total_mentions
                FROM
                    reddit_data.tech_trends t,
                    date_params d
                WHERE
                    t.week_start BETWEEN d.start_date AND d.end_date
            """

            params = [start_date, end_date]

            # Thêm filter subreddit nếu cần
            if subreddits and len(subreddits) > 0 and not (len(subreddits) == 1 and subreddits[0] == "all"):
                subreddit_list = ", ".join([f"'{s}'" for s in subreddits])
                query += f"""
                    AND EXISTS (
                        SELECT 1 
                        FROM reddit_data.subreddits s 
                        WHERE t.subreddit_id = s.subreddit_id 
                        AND s.name IN ({subreddit_list})
                    )
                """

            query += """
                GROUP BY
                    tech_name
                ORDER BY
                    total_mentions DESC
                LIMIT %s
            """

            params.append(item_count)

            # Thực hiện truy vấn
            df = pd.read_sql(query, self.db_connection, params=params)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu công nghệ nào trong khoảng thời gian được chọn',
                        'xaxis': {'title': 'Số lần đề cập'},
                        'yaxis': {'title': 'Công nghệ'}
                    }
                }

            # Sắp xếp theo số lần đề cập tăng dần (để công nghệ có nhiều đề cập nhất ở trên cùng)
            df = df.sort_values(by='total_mentions', ascending=True)

            # Tạo biểu đồ
            fig = px.bar(
                df,
                y='tech_name',
                x='total_mentions',
                title=f'Top {len(df)} công nghệ được đề cập nhiều nhất',
                labels={
                    'total_mentions': 'Số lần đề cập',
                    'tech_name': 'Công nghệ'
                },
                color='total_mentions',
                color_continuous_scale='Viridis',
                orientation='h'
            )

            # Thêm giá trị trên thanh
            fig.update_traces(
                texttemplate='%{x}',
                textposition='outside'
            )

            # Cập nhật layout
            fig.update_layout(
                height=500,
                margin=dict(l=10, r=10, t=30, b=10),
                coloraxis_showscale=False,
                xaxis=dict(title='Số lần đề cập'),
                yaxis=dict(title='Công nghệ', autorange="reversed")
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ top công nghệ: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Số lần đề cập'},
                    'yaxis': {'title': 'Công nghệ'}
                }
            }

    def _update_tech_trends_graph(self, start_date, end_date, technologies, time_unit):
        """
        Cập nhật biểu đồ xu hướng công nghệ theo thời gian
        """
        try:
            if not technologies or len(technologies) == 0:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Vui lòng chọn ít nhất một công nghệ',
                        'xaxis': {'title': 'Thời gian'},
                        'yaxis': {'title': 'Số lần đề cập'}
                    }
                }

            # Lấy dữ liệu xu hướng công nghệ
            df = self._get_tech_trends_data(start_date, end_date, technologies, time_unit)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu xu hướng công nghệ',
                        'xaxis': {'title': 'Thời gian'},
                        'yaxis': {'title': 'Số lần đề cập'}
                    }
                }

            # Tạo biểu đồ đường
            fig = px.line(
                df,
                x='time_period',
                y='mentions',
                color='tech_name',
                markers=True,
                title='Xu hướng công nghệ theo thời gian',
                labels={
                    'time_period': 'Thời gian',
                    'mentions': 'Số lần đề cập',
                    'tech_name': 'Công nghệ'
                }
            )

            # Cập nhật layout
            fig.update_layout(
                height=500,
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(title='Công nghệ', orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                xaxis=dict(title=self._get_time_unit_label(time_unit))
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ xu hướng công nghệ: {str(e)}")
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Thời gian'},
                    'yaxis': {'title': 'Số lần đề cập'}
                }
            }

    def _get_time_unit_label(self, time_unit):
        """
        Lấy nhãn cho đơn vị thời gian

        Args:
            time_unit (str): Đơn vị thời gian

        Returns:
            str: Nhãn hiển thị
        """
        labels = {
            'day': 'Ngày',
            'week': 'Tuần',
            'month': 'Tháng',
            'quarter': 'Quý'
        }
        return labels.get(time_unit, 'Thời gian')

    def _update_subreddit_trends_graph(self, start_date, end_date, subreddits, technologies):
        """
        Cập nhật biểu đồ xu hướng theo subreddit
        """
        try:
            if not technologies or len(technologies) == 0:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Vui lòng chọn ít nhất một công nghệ',
                        'xaxis': {'title': 'Subreddit'},
                        'yaxis': {'title': 'Số lần đề cập'}
                    }
                }

            # Lấy dữ liệu xu hướng theo subreddit
            df = self._get_subreddit_trends_data(start_date, end_date, subreddits, technologies)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu xu hướng theo subreddit',
                        'xaxis': {'title': 'Subreddit'},
                        'yaxis': {'title': 'Số lần đề cập'}
                    }
                }

            # Tạo biểu đồ nhóm
            fig = px.bar(
                df,
                x='subreddit_name',
                y='mentions',
                color='tech_name',
                barmode='group',
                title='Phân bố công nghệ theo subreddit',
                labels={
                    'subreddit_name': 'Subreddit',
                    'mentions': 'Số lần đề cập',
                    'tech_name': 'Công nghệ',
                    'avg_sentiment': 'Điểm tình cảm trung bình'
                },
                hover_data=['avg_sentiment']
            )

            # Cập nhật layout
            fig.update_layout(
                height=500,
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(title='Công nghệ', orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
                xaxis=dict(title='Subreddit', tickangle=-45)
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ xu hướng theo subreddit: {str(e)}")
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Subreddit'},
                    'yaxis': {'title': 'Số lần đề cập'}
                }
            }

    def _update_tech_network_graph(self, start_date, end_date, threshold, item_count):
        """
        Cập nhật biểu đồ mạng lưới tương quan công nghệ với cải tiến hiển thị
        """
        try:
            # Lấy dữ liệu tương quan công nghệ
            df = self._get_tech_correlation_data(start_date, end_date, threshold)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': f'Không có dữ liệu tương quan với ngưỡng {threshold}',
                        'xaxis': {'title': ''},
                        'yaxis': {'title': ''},
                        'autosize': True
                    }
                }

            # Tạo đồ thị mạng lưới
            G = nx.Graph()

            # Thêm cạnh vào đồ thị
            for _, row in df.iterrows():
                G.add_edge(
                    row['tech_name_1'],
                    row['tech_name_2'],
                    weight=row['correlation_score']
                )

            # Giới hạn số lượng nút (nếu cần)
            if len(G.nodes()) > item_count:
                # Lấy các nút có độ trung tâm cao nhất
                centrality = nx.degree_centrality(G)
                top_nodes = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:item_count]
                top_node_names = [node[0] for node in top_nodes]

                # Tạo đồ thị con với các nút hàng đầu
                G = G.subgraph(top_node_names)

            # Tính vị trí nút sử dụng force-directed layout
            pos = nx.spring_layout(G, seed=42)

            # Tính toán độ trung tâm và độ quan trọng của các nút
            node_degrees = dict(G.degree())

            # Sử dụng degree_centrality thay vì eigenvector_centrality_numpy
            centrality = nx.degree_centrality(G)

            # Chuẩn bị dữ liệu cho các nút
            node_x = []
            node_y = []
            node_text = []
            node_size = []
            node_color = []

            for node in G.nodes():
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                node_text.append(node)

                # Kích thước nút dựa trên số lượng kết nối
                size = 20 + 10 * node_degrees[node]
                node_size.append(size)

                # Màu sắc nút dựa trên độ quan trọng
                node_color.append(centrality[node])

            # Chuẩn bị dữ liệu cho các cạnh
            edge_traces = []

            # Định nghĩa hàm ánh xạ giá trị trọng số vào màu sắc
            def map_weight_to_color(weight):
                # Ánh xạ trọng số (thường từ 0.1 đến 1.0) vào dải màu từ xanh lam đến đỏ
                # Trọng số thấp -> Màu xanh lam, Trọng số cao -> Màu đỏ
                normalized = (weight - threshold) / (1 - threshold) if 1 - threshold > 0 else 0
                # Tạo màu theo dải từ xanh lam đến đỏ
                r = int(normalized * 255)
                g = int(50)
                b = int(255 - normalized * 255)
                return f'rgba({r},{g},{b},0.7)'

            # Tạo trace riêng cho mỗi cạnh
            for edge in G.edges(data=True):
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]

                weight = edge[2]['weight']
                # Độ rộng cạnh dựa trên trọng số
                width = 1 + 4 * weight

                # Màu sắc theo trọng số - chuyển đổi giá trị số thành màu RGB
                color = map_weight_to_color(weight)

                # Thông tin hover cho cạnh
                hover_text = f"{edge[0]} - {edge[1]}: {weight:.2f}"

                # Tạo trace riêng cho mỗi cạnh
                edge_trace = go.Scatter(
                    x=[x0, x1, None],
                    y=[y0, y1, None],
                    line=dict(
                        width=width,
                        color=color
                    ),
                    hoverinfo='text',
                    text=hover_text,
                    mode='lines'
                )
                edge_traces.append(edge_trace)

            # Tạo trace cho các nút
            node_trace = go.Scatter(
                x=node_x,
                y=node_y,
                mode='markers+text',
                text=node_text,
                textposition='top center',
                textfont=dict(size=10, color='#555'),
                marker=dict(
                    showscale=True,
                    colorscale='Viridis',
                    color=node_color,
                    size=node_size,
                    colorbar=dict(
                        thickness=15,
                        title='Độ kết nối',
                        xanchor='left',
                        titleside='right'
                    ),
                    line=dict(width=2, color='white')
                ),
                hovertemplate='<b>%{text}</b><br>Kết nối: %{marker.size}<extra></extra>'
            )

            # Tạo figure với tất cả các trace
            # Ghép tất cả các edge_traces và node_trace thành một list
            all_traces = edge_traces + [node_trace]

            fig = go.Figure(data=all_traces,
                            layout=go.Layout(
                                title=f'Mạng lưới tương quan công nghệ (Ngưỡng: {threshold})',
                                titlefont=dict(size=16),
                                showlegend=False,
                                hovermode='closest',
                                margin=dict(b=20, l=5, r=5, t=40),
                                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                plot_bgcolor='rgba(248,248,248,0.8)',
                                annotations=[
                                    dict(
                                        text="Màu sắc cạnh: Độ tương quan | Kích thước nút: Số kết nối",
                                        showarrow=False,
                                        xref="paper", yref="paper",
                                        x=0.01, y=-0.05,
                                        font=dict(size=12)
                                    )
                                ]
                            ))

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ mạng lưới tương quan: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': ''},
                    'yaxis': {'title': ''},
                    'autosize': True
                }
            }

    def _update_correlation_details(self, click_data, start_date, end_date):
        """
        Cập nhật chi tiết tương quan
        """
        if not click_data:
            return html.Div("Nhấp vào một nút trong mạng lưới để xem chi tiết tương quan.")

        try:
            # Lấy tên công nghệ từ clickData
            tech_name = click_data.get('points', [{}])[0].get('text', '')

            if not tech_name:
                return html.Div("Không thể xác định công nghệ từ lựa chọn.")

            # Truy vấn dữ liệu tương quan
            # query = """
            #     SELECT
            #         CASE
            #             WHEN tech_name_1 = %s THEN tech_name_2
            #             ELSE tech_name_1
            #         END as related_tech,
            #         correlation_score
            #     FROM
            #         reddit_data.tech_correlation
            #     WHERE
            #         (tech_name_1 = %s OR tech_name_2 = %s)
            #         AND analyzed_date BETWEEN %s AND %s
            #     ORDER BY
            #         correlation_score DESC
            #     LIMIT 10
            # """
            query = """
                SELECT 
                    CASE 
                        WHEN tech_name_1 = %s THEN tech_name_2
                        ELSE tech_name_1
                    END as related_tech,
                    correlation_score
                FROM 
                    reddit_data.tech_correlation
                WHERE 
                    (tech_name_1 = %s OR tech_name_2 = %s)
                ORDER BY 
                    correlation_score DESC
                LIMIT 10
            """

            # params = [tech_name, tech_name, tech_name, start_date, end_date]
            params = [tech_name, tech_name, tech_name]

            # Thực hiện truy vấn
            cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query, params)
            correlations = cursor.fetchall()
            cursor.close()

            if not correlations:
                return html.Div(f"Không tìm thấy tương quan nào cho công nghệ '{tech_name}'.")

            # Tạo bảng tương quan
            table_header = [
                html.Thead(html.Tr([
                    html.Th("Công nghệ liên quan"),
                    html.Th("Điểm tương quan")
                ]))
            ]

            table_rows = []
            for corr in correlations:
                row = html.Tr([
                    html.Td(corr['related_tech']),
                    html.Td(f"{corr['correlation_score']:.2f}")
                ])
                table_rows.append(row)

            table_body = [html.Tbody(table_rows)]

            return html.Div([
                html.H4(f"Công nghệ liên quan với '{tech_name}'", className="mt-2 mb-3"),
                dbc.Table(table_header + table_body, bordered=True, hover=True)
            ])

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật chi tiết tương quan: {str(e)}")
            return html.Div(f"Lỗi khi tải chi tiết tương quan: {str(e)}")

    def _update_skills_chart(self, start_date, end_date, skill_type, chart_type, item_count):
        """
        Cập nhật biểu đồ kỹ năng
        """
        try:
            # Lấy dữ liệu kỹ năng
            df = self._get_skills_data(start_date, end_date, skill_type)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu về kỹ năng',
                        'xaxis': {'title': 'Kỹ năng'},
                        'yaxis': {'title': 'Số lần đề cập'}
                    }
                }

            # Giới hạn số lượng kỹ năng hiển thị
            df = df.head(item_count)

            # Tạo biểu đồ dựa trên loại đã chọn
            if chart_type == 'bar':
                fig = px.bar(
                    df,
                    x='count',
                    y='skill',
                    orientation='h',
                    title='Kỹ năng được yêu cầu nhiều nhất',
                    labels={
                        'count': 'Số lần đề cập',
                        'skill': 'Kỹ năng'
                    },
                    color='count',
                    color_continuous_scale='Viridis'
                )

                # Cập nhật layout
                fig.update_layout(
                    height=500,
                    margin=dict(l=10, r=10, t=30, b=10),
                    coloraxis_showscale=False
                )

            else:  # treemap
                fig = px.treemap(
                    df,
                    path=['skill'],
                    values='count',
                    title='Kỹ năng được yêu cầu nhiều nhất',
                    hover_data=['count'],
                    color='count',
                    color_continuous_scale='Viridis'
                )

                # Cập nhật layout
                fig.update_layout(
                    height=500,
                    margin=dict(l=10, r=10, t=30, b=10)
                )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ kỹ năng: {str(e)}")
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Kỹ năng'},
                    'yaxis': {'title': 'Số lần đề cập'}
                }
            }

    def _update_skill_trends_graph(self, start_date, end_date, skill_type, item_count):
        """
        Cập nhật biểu đồ xu hướng kỹ năng
        """
        try:
            # Query để lấy dữ liệu xu hướng kỹ năng theo thời gian
            query = """
                        WITH date_range AS (
                            SELECT 
                                %s::date as start_date,
                                %s::date as end_date
                        )
                        SELECT
                            DATE_TRUNC('month', p.created_date)::date as month,
                            unnest(pa.skills_mentioned) as skill,
                            COUNT(*) as count
                        FROM
                            reddit_data.post_analysis pa
                            JOIN reddit_data.posts p ON pa.post_id = p.post_id,
                            date_range
                        WHERE
                            p.created_date BETWEEN date_range.start_date AND date_range.end_date
                            AND pa.skills_mentioned IS NOT NULL
                    """

            params = [start_date, end_date]

            # Thêm điều kiện lọc theo loại kỹ năng nếu cần
            if skill_type != "all":
                # Giả định rằng chúng ta có bảng kỹ năng với trường category
                # Hoặc một cách khác để lọc kỹ năng theo loại
                pass

            query += """
                        GROUP BY
                            month, skill
                        ORDER BY
                            month, count DESC
                    """

            # Thực hiện truy vấn
            df = pd.read_sql(query, self.db_connection, params=params)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu về xu hướng kỹ năng',
                        'xaxis': {'title': 'Thời gian'},
                        'yaxis': {'title': 'Số lần đề cập'}
                    }
                }

            # Lấy top kỹ năng
            top_skills = df.groupby('skill')['count'].sum().nlargest(item_count).index.tolist()

            # Lọc dữ liệu cho top kỹ năng
            df_filtered = df[df['skill'].isin(top_skills)]

            # Tạo biểu đồ đường
            fig = px.line(
                df_filtered,
                x='month',
                y='count',
                color='skill',
                markers=True,
                title='Xu hướng kỹ năng theo thời gian',
                labels={
                    'month': 'Tháng',
                    'count': 'Số lần đề cập',
                    'skill': 'Kỹ năng'
                }
            )

            # Cập nhật layout
            fig.update_layout(
                height=500,
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(title='Kỹ năng', orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ xu hướng kỹ năng: {str(e)}")
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Thời gian'},
                    'yaxis': {'title': 'Số lần đề cập'}
                }
            }

    def _update_sentiment_chart(self, start_date, end_date, chart_type, sort_by, item_count, min_mentions):
        """
        Cập nhật biểu đồ tình cảm với cải tiến hiển thị
        """
        try:
            # Lấy dữ liệu tình cảm
            df = self._get_sentiment_data(start_date, end_date, min_mentions)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu về tình cảm',
                        'xaxis': {'title': 'Công nghệ'},
                        'yaxis': {'title': 'Điểm tình cảm'},
                        'autosize': True
                    }
                }

            # Sắp xếp dữ liệu theo tiêu chí đã chọn
            if sort_by == 'sentiment':
                df = df.sort_values(by='avg_sentiment', ascending=False)
            else:  # mentions
                df = df.sort_values(by='mentions', ascending=False)

            # Giới hạn số lượng công nghệ hiển thị
            df = df.head(item_count)

            # Tạo biểu đồ dựa trên loại đã chọn
            if chart_type == 'bar':
                # Thêm màu sắc dựa trên tình cảm
                color_scale = px.colors.diverging.RdBu

                fig = px.bar(
                    df,
                    y='avg_sentiment',
                    x='tech_name',
                    title='Điểm tình cảm theo công nghệ',
                    labels={
                        'avg_sentiment': 'Điểm tình cảm',
                        'tech_name': 'Công nghệ',
                        'mentions': 'Số lần đề cập'
                    },
                    color='avg_sentiment',
                    color_continuous_scale=color_scale,
                    range_color=[-1, 1],
                    hover_data=['mentions']
                )
                # Thêm đường tham chiếu 0
                fig.add_shape(
                    type="line",
                    x0=-0.5,
                    y0=0,
                    x1=len(df) - 0.5,
                    y1=0,
                    line=dict(color="black", width=1, dash="dot")
                )

                # Cập nhật layout
                fig.update_layout(
                    autosize=True,
                    margin=dict(l=10, r=10, t=40, b=80),
                    coloraxis=dict(
                        colorbar=dict(
                            title="Điểm tình cảm",
                            tickvals=[-1, -0.5, 0, 0.5, 1],
                            ticktext=["Rất tiêu cực", "Tiêu cực", "Trung tính", "Tích cực", "Rất tích cực"]
                        )
                    ),
                    xaxis=dict(title='Công nghệ', tickangle=-45)
                )
            elif chart_type == 'heatmap':
                # Tạo ma trận cho heatmap
                tech_list = df['tech_name'].tolist()
                sentiment_matrix = np.zeros((1, len(tech_list)))

                for i, tech in enumerate(tech_list):
                    sentiment_matrix[0, i] = df[df['tech_name'] == tech]['avg_sentiment'].values[0]

                # Tạo heatmap
                fig = px.imshow(
                    sentiment_matrix,
                    x=tech_list,
                    labels=dict(x='Công nghệ', y='', color='Điểm tình cảm'),
                    color_continuous_scale='RdBu_r',
                    range_color=[-1, 1],
                    title='Điểm tình cảm theo công nghệ'
                )

                # Thêm annotations hiển thị giá trị
                annotations = []
                for i, tech in enumerate(tech_list):
                    value = sentiment_matrix[0, i]
                    color = "white" if abs(value) > 0.5 else "black"
                    annotations.append(dict(
                        x=i, y=0,
                        text=f"{value:.2f}",
                        font=dict(color=color),
                        showarrow=False
                    ))

                # Cập nhật layout
                fig.update_layout(
                    autosize=True,
                    margin=dict(l=10, r=10, t=40, b=80),
                    xaxis=dict(title='Công nghệ', tickangle=-45),
                    annotations=annotations,
                    coloraxis=dict(
                        colorbar=dict(
                            title="Điểm tình cảm",
                            tickvals=[-1, -0.5, 0, 0.5, 1],
                            ticktext=["Rất tiêu cực", "Tiêu cực", "Trung tính", "Tích cực", "Rất tích cực"]
                        )
                    )
                )

                # Xóa nhãn trục y
                fig.update_yaxes(showticklabels=False)

            else:  # radar
                # Chuẩn bị dữ liệu cho radar chart
                fig = go.Figure()

                # Thêm dữ liệu
                fig.add_trace(go.Scatterpolar(
                    r=df['avg_sentiment'].values,
                    theta=df['tech_name'].values,
                    fill='toself',
                    name='Điểm tình cảm',
                    line=dict(color='rgb(31, 119, 180)'),
                    fillcolor='rgba(31, 119, 180, 0.3)'
                ))

                # Cập nhật layout
                fig.update_layout(
                    autosize=True,
                    margin=dict(l=10, r=10, t=40, b=40),
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[-1, 1],
                            tickvals=[-1, -0.5, 0, 0.5, 1],
                            ticktext=["-1", "-0.5", "0", "0.5", "1"]
                        )
                    ),
                    title='Điểm tình cảm theo công nghệ (Radar)'
                )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ tình cảm: {str(e)}")
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Công nghệ'},
                    'yaxis': {'title': 'Điểm tình cảm'},
                    'autosize': True
                }
            }

    def _update_sentiment_trend_graph(self, start_date, end_date, technologies, time_unit):
        """
        Cập nhật biểu đồ xu hướng tình cảm theo thời gian với cải tiến hiển thị
        """
        try:
            if not technologies or len(technologies) == 0:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Vui lòng chọn ít nhất một công nghệ',
                        'xaxis': {'title': 'Thời gian'},
                        'yaxis': {'title': 'Điểm tình cảm'},
                        'autosize': True,
                        'height': 350
                    }
                }

            # Lấy dữ liệu xu hướng tình cảm
            df = self._get_tech_trends_data(start_date, end_date, technologies, time_unit)

            if df.empty:
                return {
                    'data': [],
                    'layout': {
                        'title': 'Không có dữ liệu xu hướng tình cảm',
                        'xaxis': {'title': 'Thời gian'},
                        'yaxis': {'title': 'Điểm tình cảm'},
                        'autosize': True,
                        'height': 350
                    }
                }

            # Tạo biểu đồ đường với cải tiến hiển thị
            fig = px.line(
                df,
                x='time_period',
                y='avg_sentiment',
                color='tech_name',
                markers=True,
                title='Xu hướng tình cảm theo thời gian',
                labels={
                    'time_period': 'Thời gian',
                    'avg_sentiment': 'Điểm tình cảm',
                    'tech_name': 'Công nghệ',
                    'mentions': 'Số lần đề cập'
                },
                hover_data=['mentions']
            )

            # Thêm đường tham chiếu 0
            fig.add_shape(
                type="line",
                x0=df['time_period'].min(),
                y0=0,
                x1=df['time_period'].max(),
                y1=0,
                line=dict(color="gray", width=1, dash="dot")
            )

            # Cập nhật layout với chiều cao cố định
            fig.update_layout(
                autosize=True,
                height=350,  # Chiều cao cố định
                margin=dict(l=10, r=10, t=40, b=40),
                legend=dict(
                    orientation='h',
                    yanchor='bottom',
                    y=1.02,
                    xanchor='center',
                    x=0.5
                ),
                xaxis=dict(
                    title=self._get_time_unit_label(time_unit),
                    tickangle=-30
                ),
                yaxis=dict(
                    title='Điểm tình cảm',
                    range=[-1, 1],  # Cố định phạm vi trục y
                    tickvals=[-1, -0.5, 0, 0.5, 1],
                    ticktext=["Rất tiêu cực", "Tiêu cực", "Trung tính", "Tích cực", "Rất tích cực"]
                ),
                plot_bgcolor='rgba(250,250,250,0.9)'  # Nền nhẹ
            )

            # Thêm tô màu cho vùng tích cực/tiêu cực
            fig.add_shape(
                type="rect",
                x0=df['time_period'].min(),
                y0=0,
                x1=df['time_period'].max(),
                y1=1,
                fillcolor="rgba(0,200,0,0.1)",
                line=dict(width=0),
                layer="below"
            )

            fig.add_shape(
                type="rect",
                x0=df['time_period'].min(),
                y0=-1,
                x1=df['time_period'].max(),
                y1=0,
                fillcolor="rgba(200,0,0,0.1)",
                line=dict(width=0),
                layer="below"
            )

            return fig

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật biểu đồ xu hướng tình cảm: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'data': [],
                'layout': {
                    'title': f'Lỗi khi tạo biểu đồ: {str(e)}',
                    'xaxis': {'title': 'Thời gian'},
                    'yaxis': {'title': 'Điểm tình cảm'},
                    'autosize': True,
                    'height': 350
                }
            }

    def _update_sentiment_examples(self, click_data, start_date, end_date):
        """
        Cập nhật ví dụ đánh giá với giao diện cải tiến
        """
        if not click_data:
            return html.Div(
                "Nhấp vào một công nghệ trong biểu đồ tình cảm để xem ví dụ đánh giá.",
                className="text-center text-muted p-4 h-100 d-flex align-items-center justify-content-center"
            )

        try:
            # Lấy tên công nghệ từ clickData
            tech_name = click_data.get('points', [{}])[0].get('label', '')
            if not tech_name:
                tech_name = click_data.get('points', [{}])[0].get('x', '')

            if not tech_name:
                return html.Div(
                    "Không thể xác định công nghệ từ lựa chọn.",
                    className="text-center text-muted p-4"
                )

            # Lấy ví dụ đánh giá
            df = self._get_sentiment_examples(tech_name, start_date, end_date)

            if df.empty:
                return html.Div(
                    f"Không tìm thấy ví dụ đánh giá nào cho công nghệ '{tech_name}'.",
                    className="text-center text-muted p-4"
                )

            # Chia thành đánh giá tích cực và tiêu cực
            positive_df = df[df['sentiment_score'] > 0].head(5)
            negative_df = df[df['sentiment_score'] < 0].head(5)

            # Tạo danh sách đánh giá với thiết kế cải tiến
            positive_examples = []
            negative_examples = []

            for _, row in positive_df.iterrows():
                # Tạo URL đầy đủ từ permalink
                reddit_url = f"https://www.reddit.com{row['permalink']}" if row.get('permalink') else None

                example = dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.Span(f"Điểm: ", className="text-muted small"),
                            html.Span(f"{row['sentiment_score']:.2f}",
                                      className="badge bg-success me-2"),
                            html.Span(row['created_date'].strftime('%d/%m/%Y'),
                                      className="text-muted small")
                        ], className="d-flex justify-content-between align-items-center"),
                    ], className="py-2 px-3"),
                    dbc.CardBody([
                        html.H6(
                            html.A(row['title'], href=reddit_url, target="_blank") if reddit_url else row['title'],
                            className="card-subtitle mb-2"
                        ),
                        html.P(row['text'][:150] + "..." if len(row['text']) > 150 else row['text'],
                               className="card-text small mb-1")
                    ], className="py-2 px-3"),
                    dbc.CardFooter([
                        html.A("Xem trên Reddit", href=reddit_url, target="_blank",
                               className="btn btn-sm btn-outline-success") if reddit_url else None
                    ], className="py-1 px-3 text-end") if reddit_url else None
                ], className="mb-2 border-success", style={"border-left": "4px solid var(--bs-success)"})
                positive_examples.append(example)

            for _, row in negative_df.iterrows():
                # Tạo URL đầy đủ từ permalink
                reddit_url = f"https://www.reddit.com{row['permalink']}" if row.get('permalink') else None

                example = dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.Span(f"Điểm: ", className="text-muted small"),
                            html.Span(f"{row['sentiment_score']:.2f}",
                                      className="badge bg-danger me-2"),
                            html.Span(row['created_date'].strftime('%d/%m/%Y'),
                                      className="text-muted small")
                        ], className="d-flex justify-content-between align-items-center"),
                    ], className="py-2 px-3"),
                    dbc.CardBody([
                        html.H6(
                            html.A(row['title'], href=reddit_url, target="_blank") if reddit_url else row['title'],
                            className="card-subtitle mb-2"
                        ),
                        html.P(row['text'][:150] + "..." if len(row['text']) > 150 else row['text'],
                               className="card-text small mb-1")
                    ], className="py-2 px-3"),
                    dbc.CardFooter([
                        html.A("Xem trên Reddit", href=reddit_url, target="_blank",
                               className="btn btn-sm btn-outline-danger") if reddit_url else None
                    ], className="py-1 px-3 text-end") if reddit_url else None
                ], className="mb-2 border-danger", style={"border-left": "4px solid var(--bs-danger)"})
                negative_examples.append(example)

            return html.Div([
                html.H4(f"Đánh giá về '{tech_name}'", className="mb-3 text-primary"),

                dbc.Row([
                    dbc.Col([
                        html.H5([
                            html.I(className="fas fa-thumbs-up text-success me-2"),
                            "Đánh giá tích cực"
                        ], className="text-success h6 mb-3"),
                        html.Div(positive_examples if positive_examples else
                                 html.Div("Không có đánh giá tích cực.",
                                          className="text-center text-muted p-3 border rounded"))
                    ], md=6),

                    dbc.Col([
                        html.H5([
                            html.I(className="fas fa-thumbs-down text-danger me-2"),
                            "Đánh giá tiêu cực"
                        ], className="text-danger h6 mb-3"),
                        html.Div(negative_examples if negative_examples else
                                 html.Div("Không có đánh giá tiêu cực.",
                                          className="text-center text-muted p-3 border rounded"))
                    ], md=6)
                ])
            ], className="h-100 p-2")  # Đảm bảo sử dụng toàn bộ chiều cao

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật ví dụ đánh giá: {str(e)}")
            return html.Div(
                f"Lỗi khi tải ví dụ đánh giá: {str(e)}",
                className="text-danger p-3"
            )


    def run_server(self, host="0.0.0.0", port=8050, debug=False):
        """
        Chạy Dash server

        Args:
            host (str): Host address
            port (int): Port number
            debug (bool): Debug mode
        """
        self.app.run(host=host, port=port, debug=debug)

    def shutdown(self):
        """
        Đóng kết nối và giải phóng tài nguyên
        """
        if hasattr(self, 'db_connection') and self.db_connection:
            self.db_connection.close()
            logger.info("Đã đóng kết nối database")






