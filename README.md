 <!--Database yang digunakan adalah database local jadi silahkan untuk menyesuaikan jika ingin test  -->
# Lion Parcel Bonus ETL Pipeline

ETL pipeline untuk mengambil data bonus Lion Parcel dari Google Drive, memproses dengan DBT, dan load ke PostgreSQL menggunakan Apache Airflow.

## 📋 Prerequisites

- Docker Desktop atau Docker Engine (v20.10+)
- Docker Compose (v2.0+)
- Git
- Minimal 4GB RAM untuk Docker
- Port 8080 dan 5432 harus tersedia

## Arsitektur

```
┌─────────────────┐
│  Google Drive   │
│   (JSON Files)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  Python Script  │─────▶│  PostgreSQL  │
│  (ETL Process)  │      │   (Staging)  │
└────────┬────────┘      └──────┬───────┘
         │                      │
         ▼                      ▼
┌─────────────────┐      ┌──────────────┐
│   DBT Models    │─────▶│  PostgreSQL  │
│ (Transform SQL) │      │    (Prod)    │
└─────────────────┘      └──────────────┘
         │
         ▼
┌─────────────────┐
│ Apache Airflow  │
│  (Orchestrator) │
└─────────────────┘
```

## Struktur Project

```
.
├── Dockerfile.airflow       # Dockerfile untuk Airflow
├── docker-compose.yml        # Orchestration services
├── requirements.txt          # Python dependencies
├── README.md                 # Dokumentasi ini
├── dags/                     # Airflow DAGs
│   └── dag_lion_parcel.py
├── etl/                      # ETL scripts
│   ├── connection.py         # DB connection helper
│   └── extract_load.py       # Script download & load data
├── dbt/                      # DBT project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       └── lion_parcel_bonus_test.sql
├── logs/                     # Airflow logs (auto-created)
└── downloads_json/           # Temp folder untuk file JSON (auto-created)
```

## 🚀 Cara Setup & Run

### 1. Clone Repository

```bash
git clone https://github.com/username/lion-parcel-etl.git
cd lion-parcel-etl
```

### 2. Konfigurasi Environment

Edit `docker-compose.yml` untuk sesuaikan kredensial database production:

```yaml
environment:
  # ... env lainnya
  DWH_USER: postgres           # ⬅️ ganti sesuai DB prod
  DWH_PASSWORD: postgres       # ⬅️ ganti sesuai DB prod
  DWH_HOST: host.docker.internal
  DWH_PORT: "5432"
  DWH_DATABASE: postgres       # ⬅️ ganti sesuai DB prod
```

**Penting**: 
- Pakai `host.docker.internal` untuk akses DB lokal dari dalam container
- Atau pakai IP host jika DB di server lain

### 3. Build Docker Image

```bash
# Build image pertama kali (tanpa cache)
docker compose build --no-cache --progress=plain
```

**Proses ini akan**:
- Download base image Apache Airflow 2.9.3
- Install system dependencies (unixodbc-dev)
- Install semua Python packages dari requirements.txt
- Estimasi waktu: 5-10 menit

### 4. Start Services

```bash
# Start semua container
docker compose up -d

# Cek status containers
docker compose ps
```

**Services yang akan jalan**:
- `postgres` - Database untuk Airflow metadata (port 5432)
- `airflow-init` - Initialize Airflow DB & create admin user
- `airflow-webserver` - Web UI (port 8080)
- `airflow-scheduler` - Task scheduler
- `airflow-triggerer` - Trigger handler

### 5. Akses Airflow Web UI

1. Buka browser: http://localhost:8080
2. Login credentials:
   - **Username**: `admin`
   - **Password**: `admin`

### 6. Jalankan DAG

1. Di Airflow UI, cari DAG: `lion_parcel_bonus_pipeline`
2. Toggle switch untuk **enable** DAG
3. Klik tombol **▶ Play** untuk trigger manual run
4. Monitor progress di **Graph View** atau **Logs**

## 📊 Data Flow

### Task 1: Extract & Load (Python)
- Download JSON files dari Google Drive public folder
- Parse dan aggregate data (1 row per id)
- Load ke staging table: `public.lion_parcell_bonus_test_stg`

### Task 2: DBT Transform (SQL)
- Transform data dari staging
- Apply business logic
- Load ke production table: `public.lion_parcell_bonus_test`

## 🛠️ Troubleshooting

### Container gagal start

```bash
# Cek logs semua services
docker compose logs

# Cek logs specific service
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
```

### Database connection error

Pastikan:
1. PostgreSQL production sudah running
2. Credentials di `docker-compose.yml` benar
3. Port 5432 tidak di-block firewall
4. Pakai `host.docker.internal` untuk akses localhost dari container

### Library missing error

```bash
# Rebuild image dengan --no-cache
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Reset semua data

```bash
# Stop dan hapus semua container + volumes
docker compose down -v

# Start ulang dari awal
docker compose up -d
```

## 🔧 Development

### Tambah Python Package

1. Edit `requirements.txt`
2. Rebuild image:
```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Edit DAG

File DAG di folder `dags/` akan auto-reload tanpa restart container (volume mount).

### Edit DBT Model

1. Edit file SQL di `dbt/models/`
2. DBT akan jalankan model terbaru saat task running

## 📦 Volumes (Data Persistence)

Data yang di-persist di local disk:

```yaml
volumes:
  - ./dags:/opt/airflow/dags          # DAG files
  - ./logs:/opt/airflow/logs          # Airflow logs
  - ./dbt:/opt/airflow/dbt            # DBT project
  - ./etl:/opt/airflow/etl            # ETL scripts
  - postgres-db-volume:/var/lib/postgresql/data  # Airflow metadata DB
```

**Data yang TIDAK hilang saat container restart**:
- Airflow logs
- Airflow metadata (DAG runs, task instances)
- Code (DAGs, DBT models, ETL scripts)

**Data yang HILANG saat `docker compose down -v`**:
- Downloaded JSON files (di `downloads_json/`)
- Data di staging/prod table (ada di DB external, bukan di container)

## 🧹 Cleanup

### Stop services (keep data)
```bash
docker compose down
```

### Stop services + hapus volumes
```bash
docker compose down -v
```

### Hapus images
```bash
docker compose down -v
docker rmi $(docker images -q lion-parcel-etl*)
```

## 📝 Requirements

Library yang dipakai (dari `requirements.txt`):

- **Database**: `psycopg2-binary`, `sqlalchemy`
- **DBT**: `dbt-core`, `dbt-postgres`
- **Data Processing**: `pandas`, `pyarrow`
- **Web Scraping**: `requests`, `beautifulsoup4`
- **Excel**: `openpyxl`

## 🤝 Contributing

1. Fork repository
2. Create feature branch: `git checkout -b feature/nama-fitur`
3. Commit changes: `git commit -m 'Add fitur baru'`
4. Push to branch: `git push origin feature/nama-fitur`
5. Submit Pull Request

## 📄 License

MIT License - lihat file LICENSE untuk detail

## 👤 Author

Nama Kamu - [GitHub Profile](https://github.com/username)

## 🔗 Links

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [DBT Docs](https://docs.getdbt.com/)
- [Docker Compose Docs](https://docs.docker.com/compose/)