# Cấu trúc thư mục:
nspw_docker2/
├── app/
│   └── main.py
├── data/
│   └── test.csv
├── results/
│   └── 
├── tests/
│   └── test_weather_api.ipynb
│
└── dockerfile
└── README.md
└── requirements.txt
└── .gitignore

# Xây dựng Docker Image:
docker build -t nspw-api2 .

# Chạy Container:
docker run -d -p 8000:8000 nspw-api2

# Test API
curl -X POST "http://127.0.0.1:8000/weather" \
-F "file=@data/test.csv" \
-F "start_year=2014" \
-F "end_year=2024" \
-F "output_format=json" \
-o results/test.json

curl -X POST "http://127.0.0.1:8000/weather" \
-F "file=@data/test.csv" \
-F "start_year=2014" \
-F "end_year=2024" \
-F "output_format=bz2" \
-o results/result_test.json.bz2
