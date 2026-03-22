CREATE TABLE IF NOT EXISTS weather_data (
    id                 SERIAL PRIMARY KEY,           -- 자동 증가하는 단일 기본키
    date               DATE NOT NULL,                -- 날짜
    city               VARCHAR(50) NOT NULL,         -- 도시명
    temperature        NUMERIC(4, 1),                -- 기온
    humidity           INTEGER,                      -- 습도
    wind_speed         NUMERIC(4, 1),                -- 풍속
    precipitation_prob INTEGER,                      -- 강수확률
    apparent_temp      NUMERIC(4, 1),                -- 체감온도
    weather_status     VARCHAR(100)                 -- 날씨 상태
);