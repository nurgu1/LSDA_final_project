
DROP TABLE IF EXISTS risk_report;
-- Create the External Table schema pointing to S3 Parquet data
CREATE EXTERNAL TABLE IF NOT EXISTS risk_report (
  flight_id STRING,
  origin STRING,
  destination STRING,
  tail_number STRING,
  plane_age INT,
  temperature DOUBLE,
  windspeed DOUBLE,
  sunset STRING,
  scheduled_arr STRING,
  risk_score INT,
  prediction STRING
)
STORED AS PARQUET
LOCATION 's3://flight-delay-project-mglgx7/curated/risk_report/';


-- KPI 1: Hub Vulnerability Index (Operational Strategy)
-- Goal: Identify high-risk network hubs to prioritize crew allocation.
SELECT 
    origin as hub_airport,
    COUNT(*) as total_departures,
    ROUND(SUM(CASE WHEN prediction = 'HIGH_RISK' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as high_risk_percentage,
    ROUND(AVG(risk_score), 1) as avg_hub_risk,
    SUM(risk_score) as total_accumulated_risk
FROM risk_report
GROUP BY origin
HAVING COUNT(*) > 50  -- Filter for major hubs
ORDER BY total_accumulated_risk DESC;


-- KPI 2: Strategic Asset Management (Fleet Vulnerability)
-- Goal: Validate if older aircraft (>20y) carry higher risk scores than modern ones.
SELECT 
    CASE 
        WHEN plane_age > 20 THEN 'Aging Fleet (>20y)'
        WHEN plane_age BETWEEN 10 AND 20 THEN 'Mid-Life (10-20y)'
        ELSE 'Modern Fleet (<10y)'
    END as fleet_generation,
    COUNT(*) as flight_count,
    ROUND(AVG(risk_score), 1) as avg_risk_score
FROM risk_report
GROUP BY 
    CASE 
        WHEN plane_age > 20 THEN 'Aging Fleet (>20y)'
        WHEN plane_age BETWEEN 10 AND 20 THEN 'Mid-Life (10-20y)'
        ELSE 'Modern Fleet (<10y)'
    END
ORDER BY avg_risk_score DESC;


-- KPI 3: Day vs. Night Operational Risk (Solar Impact)
-- Goal: Measure the risk differential between day and night operations.
SELECT 
    CASE 
        WHEN scheduled_arr > '18:00' THEN 'Night Operation'
        ELSE 'Day Operation'
    END as flight_type,
    COUNT(*) as total_flights,
    ROUND(AVG(risk_score), 1) as avg_risk,
    SUM(CASE WHEN prediction = 'HIGH_RISK' THEN 1 ELSE 0 END) as critical_alerts
FROM risk_report
GROUP BY 1
ORDER BY avg_risk DESC;


-- KPI 4: Analytical Validity (Model Sensitivity)
-- Goal: Statistical validation using Pearson Correlation.
SELECT 
    CORR(windspeed, risk_score) as weather_correlation,
    CORR(plane_age, risk_score) as fleet_age_correlation
FROM risk_report;
