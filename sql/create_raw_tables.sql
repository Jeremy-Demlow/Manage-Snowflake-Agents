-- ============================================================================
-- SKI RESORT DATABASE - COMPREHENSIVE RAW TABLES
-- World-class data model for ski resort analytics
-- ============================================================================

USE DATABASE SKI_RESORT_DB;
USE SCHEMA RAW;

-- ============================================================================
-- REFERENCE/DIMENSION TABLES
-- ============================================================================

-- Customers - Enhanced with acquisition and lifetime metrics
CREATE OR REPLACE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    birth_date DATE,
    customer_segment VARCHAR(50),
    is_pass_holder BOOLEAN,
    pass_type VARCHAR(20),
    first_visit_date DATE,
    home_zip_code VARCHAR(20),
    state VARCHAR(10),
    -- New acquisition fields
    acquisition_date DATE,
    acquisition_channel VARCHAR(30),
    acquisition_campaign_id VARCHAR(30),
    acquisition_source VARCHAR(50),
    -- New lifetime metrics
    lifetime_value NUMBER(10,2),
    total_visits NUMBER,
    total_spend NUMBER(10,2),
    avg_spend_per_visit NUMBER(10,2),
    last_visit_date DATE,
    churn_risk_score NUMBER(3,2),
    -- Communication preferences
    preferred_channel VARCHAR(30),
    email_opt_in BOOLEAN DEFAULT TRUE,
    sms_opt_in BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Lifts - Reference data
CREATE OR REPLACE TABLE lifts (
    lift_id VARCHAR(20) PRIMARY KEY,
    lift_name VARCHAR(100),
    lift_type VARCHAR(50),
    capacity_per_hour NUMBER,
    vertical_feet NUMBER,
    terrain_type VARCHAR(50),
    base_elevation NUMBER,
    top_elevation NUMBER,
    difficulty_access VARCHAR(50)
);

-- Locations - Reference data
CREATE OR REPLACE TABLE locations (
    location_id VARCHAR(20) PRIMARY KEY,
    location_name VARCHAR(100),
    location_type VARCHAR(50),
    venue_size VARCHAR(20),
    seating_capacity NUMBER,
    elevation_zone VARCHAR(20)
);

-- Products - Reference data
CREATE OR REPLACE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    product_type VARCHAR(50),
    price NUMBER(10, 2)
);

-- Ticket types - Reference data
CREATE OR REPLACE TABLE ticket_types (
    ticket_type_id VARCHAR(20) PRIMARY KEY,
    ticket_name VARCHAR(100),
    ticket_category VARCHAR(50),
    duration_days NUMBER,
    access_level VARCHAR(50),
    price NUMBER(10, 2),
    blackout_dates VARCHAR(200)
);

-- Instructors - Ski school staff
CREATE OR REPLACE TABLE instructors (
    instructor_id VARCHAR(20) PRIMARY KEY,
    instructor_name VARCHAR(100) NOT NULL,
    certification_level VARCHAR(30),
    specialties VARCHAR(200),
    languages VARCHAR(100),
    sport_type VARCHAR(20) DEFAULT 'Ski',
    hire_date DATE,
    hourly_rate NUMBER(6,2),
    avg_rating NUMBER(3,2),
    total_lessons NUMBER,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Parking lots - Reference data
CREATE OR REPLACE TABLE parking_lots (
    lot_id VARCHAR(20) PRIMARY KEY,
    lot_name VARCHAR(50) NOT NULL,
    lot_type VARCHAR(30),
    total_spaces NUMBER(5) NOT NULL,
    hourly_rate NUMBER(6,2),
    daily_max NUMBER(6,2),
    distance_to_lifts_feet NUMBER(6),
    has_shuttle BOOLEAN DEFAULT FALSE,
    elevation_zone VARCHAR(30),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Employees - Full roster
CREATE OR REPLACE TABLE employees (
    employee_id VARCHAR(20) PRIMARY KEY,
    employee_name VARCHAR(100) NOT NULL,
    department VARCHAR(50) NOT NULL,
    job_title VARCHAR(50) NOT NULL,
    hire_date DATE NOT NULL,
    termination_date DATE,
    employment_type VARCHAR(20),
    hourly_rate NUMBER(6,2),
    certifications TEXT,
    emergency_contact VARCHAR(100),
    home_zip VARCHAR(10),
    is_supervisor BOOLEAN DEFAULT FALSE,
    reports_to VARCHAR(20),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- TRANSACTIONAL TABLES - EXISTING (ENHANCED)
-- ============================================================================

-- Lift scans - Enhanced
CREATE OR REPLACE TABLE lift_scans (
    scan_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    lift_id VARCHAR(20),
    scan_timestamp TIMESTAMP_NTZ,
    wait_time_minutes NUMBER(5,1),
    temperature_f NUMBER,
    weather_condition VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Pass usage
CREATE OR REPLACE TABLE pass_usage (
    usage_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50),
    visit_date DATE,
    first_scan_time TIMESTAMP_NTZ,
    last_scan_time TIMESTAMP_NTZ,
    total_lift_rides NUMBER,
    hours_on_mountain NUMBER(5, 2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Ticket sales - Day passes and multi-day
CREATE OR REPLACE TABLE ticket_sales (
    sale_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    ticket_type_id VARCHAR(20),
    location_id VARCHAR(20),
    purchase_timestamp TIMESTAMP_NTZ,
    valid_from_date DATE,
    valid_to_date DATE,
    purchase_amount NUMBER(10, 2),
    payment_method VARCHAR(50),
    purchase_channel VARCHAR(50),
    promo_code VARCHAR(30),
    campaign_id VARCHAR(30),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Season pass sales - NEW: High-value customer tracking
CREATE OR REPLACE TABLE season_pass_sales (
    sale_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    ticket_type_id VARCHAR(10) NOT NULL,
    purchase_date DATE NOT NULL,
    valid_season VARCHAR(10) NOT NULL,
    purchase_amount NUMBER(10,2) NOT NULL,
    original_price NUMBER(10,2),
    discount_amount NUMBER(10,2) DEFAULT 0,
    payment_method VARCHAR(20),
    purchase_channel VARCHAR(20),
    is_renewal BOOLEAN DEFAULT FALSE,
    previous_pass_type VARCHAR(10),
    promo_code VARCHAR(30),
    campaign_id VARCHAR(30),
    payment_plan BOOLEAN DEFAULT FALSE,
    payment_plan_months NUMBER(2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Rentals
CREATE OR REPLACE TABLE rentals (
    rental_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    location_id VARCHAR(20),
    product_id VARCHAR(20),
    rental_timestamp TIMESTAMP_NTZ,
    return_timestamp TIMESTAMP_NTZ,
    rental_duration_hours NUMBER(5, 2),
    rental_amount NUMBER(10, 2),
    deposit_amount NUMBER(10,2),
    damage_reported BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Food and beverage
CREATE OR REPLACE TABLE food_beverage (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    location_id VARCHAR(20),
    product_id VARCHAR(20),
    transaction_timestamp TIMESTAMP_NTZ,
    quantity NUMBER,
    unit_price NUMBER(10, 2),
    total_amount NUMBER(10, 2),
    payment_method VARCHAR(50),
    is_combo BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- MARKETING TABLES - NEW
-- ============================================================================

-- Marketing campaigns - Campaign-level data with spend
CREATE OR REPLACE TABLE marketing_campaigns (
    campaign_id VARCHAR(30) PRIMARY KEY,
    campaign_name VARCHAR(100) NOT NULL,
    campaign_type VARCHAR(30) NOT NULL,
    channel VARCHAR(30) NOT NULL,
    target_audience VARCHAR(50),
    start_date DATE NOT NULL,
    end_date DATE,
    budget NUMBER(10,2),
    actual_spend NUMBER(10,2),
    impressions NUMBER,
    clicks NUMBER,
    unique_visitors NUMBER,
    conversions NUMBER,
    revenue_attributed NUMBER(10,2),
    cost_per_acquisition NUMBER(10,2),
    return_on_ad_spend NUMBER(5,2),
    creative_variant VARCHAR(50),
    landing_page VARCHAR(200),
    utm_source VARCHAR(50),
    utm_medium VARCHAR(50),
    utm_campaign VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Customer campaign touches - Individual interactions
CREATE OR REPLACE TABLE customer_campaign_touches (
    touch_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    campaign_id VARCHAR(30) NOT NULL,
    touch_date DATE NOT NULL,
    touch_timestamp TIMESTAMP_NTZ,
    channel VARCHAR(30),
    was_delivered BOOLEAN DEFAULT TRUE,
    was_opened BOOLEAN DEFAULT FALSE,
    open_timestamp TIMESTAMP_NTZ,
    was_clicked BOOLEAN DEFAULT FALSE,
    click_timestamp TIMESTAMP_NTZ,
    click_url VARCHAR(500),
    converted BOOLEAN DEFAULT FALSE,
    conversion_timestamp TIMESTAMP_NTZ,
    conversion_type VARCHAR(30),
    conversion_value NUMBER(10,2),
    conversion_sale_id VARCHAR(30),
    unsubscribed BOOLEAN DEFAULT FALSE,
    bounce_type VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Legacy marketing touches (keep for backwards compatibility)
CREATE OR REPLACE TABLE marketing_touches (
    touch_id VARCHAR(60) PRIMARY KEY,
    campaign_id VARCHAR(40),
    campaign_name VARCHAR(150),
    campaign_channel VARCHAR(50),
    campaign_type VARCHAR(50),
    audience_segment VARCHAR(100),
    send_date DATE,
    target_count NUMBER(10, 0),
    open_rate NUMBER(5, 4),
    click_rate NUMBER(5, 4),
    conversion_count NUMBER(10, 0),
    revenue_attributed NUMBER(12, 2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- OPERATIONAL TABLES - EXISTING (ENHANCED)
-- ============================================================================

-- Weather conditions
CREATE OR REPLACE TABLE weather_conditions (
    weather_date DATE,
    mountain_zone VARCHAR(100),
    snow_condition VARCHAR(50),
    snowfall_inches NUMBER(5, 2),
    base_depth_inches NUMBER(5, 2),
    temp_high_f NUMBER(5, 1),
    temp_low_f NUMBER(5, 1),
    wind_speed_mph NUMBER(5, 1),
    visibility VARCHAR(20),
    uv_index NUMBER(2),
    storm_warning BOOLEAN,
    avalanche_risk VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (weather_date, mountain_zone)
);

-- Staffing schedule
CREATE OR REPLACE TABLE staffing_schedule (
    schedule_id VARCHAR(60) PRIMARY KEY,
    schedule_date DATE,
    location_id VARCHAR(20),
    department VARCHAR(100),
    job_role VARCHAR(100),
    employee_id VARCHAR(20),
    scheduled_employees NUMBER(5, 0),
    actual_employees NUMBER(5, 0),
    coverage_ratio NUMBER(5, 2),
    shift_start TIMESTAMP_NTZ,
    shift_end TIMESTAMP_NTZ,
    overtime_hours NUMBER(4,1),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- OPERATIONAL TABLES - NEW
-- ============================================================================

-- Ski lessons
CREATE OR REPLACE TABLE ski_lessons (
    lesson_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    lesson_date DATE NOT NULL,
    lesson_start_time TIME,
    lesson_type VARCHAR(30) NOT NULL,
    sport_type VARCHAR(20) DEFAULT 'Ski',
    skill_level VARCHAR(20),
    duration_hours NUMBER(3,1) NOT NULL,
    instructor_id VARCHAR(20),
    group_size NUMBER(2),
    lesson_amount NUMBER(10,2) NOT NULL,
    rental_included BOOLEAN DEFAULT FALSE,
    rental_amount NUMBER(10,2),
    tip_amount NUMBER(10,2),
    booking_channel VARCHAR(20),
    booking_lead_days NUMBER(3),
    completed BOOLEAN DEFAULT TRUE,
    cancellation_reason VARCHAR(100),
    student_rating NUMBER(2,1),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Incidents
CREATE OR REPLACE TABLE incidents (
    incident_id VARCHAR(30) PRIMARY KEY,
    incident_date DATE NOT NULL,
    incident_time TIME,
    incident_timestamp TIMESTAMP_NTZ,
    incident_type VARCHAR(30) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    location_id VARCHAR(20),
    lift_id VARCHAR(10),
    trail_name VARCHAR(50),
    customer_id VARCHAR(20),
    customer_age NUMBER(3),
    customer_skill_level VARCHAR(20),
    description TEXT,
    cause VARCHAR(100),
    weather_factor BOOLEAN DEFAULT FALSE,
    equipment_factor BOOLEAN DEFAULT FALSE,
    first_aid_rendered BOOLEAN DEFAULT FALSE,
    transport_required BOOLEAN DEFAULT FALSE,
    transport_type VARCHAR(30),
    patrol_response_minutes NUMBER(4,1),
    resolution VARCHAR(200),
    followup_required BOOLEAN DEFAULT FALSE,
    report_filed BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Customer feedback
CREATE OR REPLACE TABLE customer_feedback (
    feedback_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20),
    feedback_date DATE NOT NULL,
    feedback_type VARCHAR(30) NOT NULL,
    survey_id VARCHAR(30),
    nps_score NUMBER(2),
    satisfaction_score NUMBER(2),
    likelihood_to_return NUMBER(2),
    likelihood_to_recommend NUMBER(2),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    sentiment VARCHAR(20),
    sentiment_score NUMBER(3,2),
    feedback_text TEXT,
    response_text TEXT,
    response_date DATE,
    responded_by VARCHAR(50),
    resolved BOOLEAN DEFAULT FALSE,
    resolution_date DATE,
    escalated BOOLEAN DEFAULT FALSE,
    source VARCHAR(30),
    visit_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Parking occupancy
CREATE OR REPLACE TABLE parking_occupancy (
    record_id VARCHAR(30) PRIMARY KEY,
    record_date DATE NOT NULL,
    record_hour NUMBER(2) NOT NULL,
    lot_id VARCHAR(20) NOT NULL,
    lot_name VARCHAR(50),
    total_spaces NUMBER(5),
    occupied_spaces NUMBER(5),
    occupancy_percent NUMBER(5,2),
    vehicles_entered NUMBER(5),
    vehicles_exited NUMBER(5),
    revenue_collected NUMBER(10,2),
    overflow_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Lift maintenance
CREATE OR REPLACE TABLE lift_maintenance (
    maintenance_id VARCHAR(30) PRIMARY KEY,
    lift_id VARCHAR(10) NOT NULL,
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(30) NOT NULL,
    category VARCHAR(50),
    description TEXT,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    downtime_minutes NUMBER(6),
    during_operating_hours BOOLEAN DEFAULT FALSE,
    parts_replaced TEXT,
    parts_cost NUMBER(10,2),
    labor_hours NUMBER(5,1),
    labor_cost NUMBER(10,2),
    total_cost NUMBER(10,2),
    technician_id VARCHAR(20),
    passed_inspection BOOLEAN,
    followup_required BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Grooming logs
CREATE OR REPLACE TABLE grooming_logs (
    log_id VARCHAR(30) PRIMARY KEY,
    grooming_date DATE NOT NULL,
    shift VARCHAR(20),
    trail_name VARCHAR(50) NOT NULL,
    groomer_id VARCHAR(20),
    machine_id VARCHAR(20),
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_minutes NUMBER(4),
    grooming_type VARCHAR(30),
    snow_depth_inches NUMBER(4,1),
    conditions_before VARCHAR(50),
    conditions_after VARCHAR(50),
    fuel_used_gallons NUMBER(5,1),
    notes TEXT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'Tables created successfully!' AS status;
SHOW TABLES IN SCHEMA SKI_RESORT_DB.RAW;
