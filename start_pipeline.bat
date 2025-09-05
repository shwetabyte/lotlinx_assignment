@echo off
echo ====================================
echo Starting NHTSA Data Pipeline
echo ====================================

echo Step 1: Stopping any existing containers...
docker-compose down

echo Step 2: Starting all services...
docker-compose up -d

echo Step 3: Waiting for services to start...
timeout /t 30 /nobreak

echo Step 4: Checking container status...
docker-compose ps

echo ====================================
echo Pipeline Services Started!
echo ====================================
echo.
echo Access Points:
echo - Airflow UI: http://localhost:8080 (admin/admin123)
echo - PgAdmin:    http://localhost:8081 (admin@nhtsa.com/admin123)
echo - PostgreSQL: localhost:5432 (nhtsa_user/nhtsa_password)
echo.
echo To stop: docker-compose down
echo To view logs: docker-compose logs [service-name]
echo ====================================


