-- ──────────────────────────────────────────────────────────────────────────
-- sdd-kafka-snowflake v2 — inicialização do PostgreSQL fonte
-- Plataforma: delivery de comida (mercado brasileiro)
--
-- ESCOPO: só os 10 domínios Tier 1 — os que alimentam Silver/Gold.
-- O projeto anterior criava 20 tabelas; os 10 Tier 2 (payments, gps_events,
-- order_status, routes, receipts, support_tickets, products, menu_sections,
-- ratings, inventory) foram removidos por DEFINE_MIGRACAO_INGESTAO_V4
-- ("metade dos 20 domínios nunca alimenta nenhum modelo Silver ou Gold,
-- sendo volume pago e processado sem consumo analítico real").
--
-- Os 10 aqui batem 1:1 com os 10 Streams/Tasks de scripts/streams_and_tasks.sql.
--
-- Ausência de FK entre tabelas é deliberada e vem do projeto anterior: com
-- CDC por tabela, a ordem de chegada dos eventos não respeita integridade
-- referencial, e uma FK aqui quebraria a carga. Os *_key são referências
-- lógicas (CPF, CNPJ, driver_id), validadas na Silver, não no Postgres.
-- ──────────────────────────────────────────────────────────────────────────

-- ════════════════════════════════════════════════════════════════════════════
-- 1. PAYMENT EVENTS (kafka_events) — event sourcing
--    Objeto de evento aninhado: {event_name, timestamp (int ou float)}
--    Ciclo CDC: created→authorized→captured→succeeded→settled→closed
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS payment_events (
    event_id             UUID        NOT NULL,
    payment_id           UUID        NOT NULL,
    event                JSONB       NOT NULL,  -- {event_name, timestamp}
    dt_current_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_payment_events PRIMARY KEY (event_id)
);
CREATE INDEX IF NOT EXISTS idx_payment_events_payment_id
    ON payment_events (payment_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 2. ORDERS (kafka_orders) — entidade
--    Tabela hub — liga todos os domínios via referências *_key
--    Chaves usam identificadores de negócio: CPF (user), CNPJ (restaurant),
--    driver_id. `rating_key` permanece na tabela mas aponta para um domínio
--    Tier 2 removido — fica como coluna órfã até a Silver decidir descartá-la.
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS orders (
    order_id             UUID           NOT NULL,
    order_date           TIMESTAMPTZ,
    total_amount         NUMERIC(10,2),
    user_key             VARCHAR(20),   -- formato CPF: 000.000.000-00
    restaurant_key       VARCHAR(20),   -- formato CNPJ: 00.000.000/0000-00
    driver_key           VARCHAR(20),   -- driver_id (string)
    payment_key          UUID,          -- referência a payment_id
    rating_key           UUID,          -- domínio Tier 2 removido
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_user_key       ON orders (user_key);
CREATE INDEX IF NOT EXISTS idx_orders_restaurant_key ON orders (restaurant_key);
CREATE INDEX IF NOT EXISTS idx_orders_driver_key     ON orders (driver_key);
CREATE INDEX IF NOT EXISTS idx_orders_date           ON orders (order_date);

-- ════════════════════════════════════════════════════════════════════════════
-- 3. ORDER ITEMS (mongodb_items) — fato, maior tabela do projeto
--    Itens de linha por pedido: produto, quantidade, preço, modificadores
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id        UUID           NOT NULL,
    order_id             UUID,
    product_id           VARCHAR(20),   -- formato PRD-XXXXX
    restaurant_id        INTEGER,
    product_name         VARCHAR(200),
    product_type         VARCHAR(50),
    cuisine_type         VARCHAR(50),
    unit_price           NUMERIC(10,2),
    quantity             INTEGER,
    subtotal             NUMERIC(10,2),
    discount_applied     NUMERIC(10,2),
    modifiers            VARCHAR(500),  -- string simples, ex: "sem gelo"
    is_combo             BOOLEAN,
    is_vegetarian        BOOLEAN,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_order_items PRIMARY KEY (order_item_id)
);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id      ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id    ON order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_restaurant_id ON order_items (restaurant_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 4. DRIVER SHIFTS (kafka_shift) — entidade
--    Desempenho por turno: ganhos, distância, pedidos, avaliação
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS driver_shifts (
    shift_id             UUID           NOT NULL,
    driver_id            VARCHAR(20),
    city                 VARCHAR(100),
    region               VARCHAR(100),
    shift_type           VARCHAR(20),   -- full-time, part-time
    login_method         VARCHAR(30),
    device_os            VARCHAR(30),
    start_time           TIMESTAMPTZ,
    end_time             TIMESTAMPTZ,
    shift_duration_min   INTEGER,
    num_orders           INTEGER,
    distance_covered_km  NUMERIC(8,2),
    earnings_brl         NUMERIC(10,2),
    shift_rating         NUMERIC(3,2),
    issues_reported      VARCHAR(100),  -- categórico: 'Late Start', 'App Crash', 'None'
    available            BOOLEAN,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_driver_shifts PRIMARY KEY (shift_id)
);
CREATE INDEX IF NOT EXISTS idx_driver_shifts_driver_id  ON driver_shifts (driver_id);
CREATE INDEX IF NOT EXISTS idx_driver_shifts_start_time ON driver_shifts (start_time);

-- ════════════════════════════════════════════════════════════════════════════
-- 5. SEARCH EVENTS (kafka_search) — event sourcing
--    Buscas do usuário (filters é string simples, não aninhado)
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS search_events (
    search_id            UUID           NOT NULL,
    user_id              INTEGER,
    query_text           TEXT,
    filters              VARCHAR(200),
    result_count         INTEGER,
    clicked_product_id   VARCHAR(20),
    timestamp            TIMESTAMPTZ,
    CONSTRAINT pk_search_events PRIMARY KEY (search_id)
);
CREATE INDEX IF NOT EXISTS idx_search_events_user_id ON search_events (user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 6. RECOMMENDATIONS (mongodb_recommendations) — event sourcing
--    Eventos de recomendação de ML: view, click, purchase, dismiss
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS recommendations (
    event_id             UUID           NOT NULL,
    user_id              INTEGER,
    product_id           VARCHAR(20),
    event_type           VARCHAR(50),   -- view, click, purchase, dismiss
    timestamp            TIMESTAMPTZ,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_recommendations PRIMARY KEY (event_id)
);
CREATE INDEX IF NOT EXISTS idx_recommendations_user_id    ON recommendations (user_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_product_id ON recommendations (product_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 7. USERS — fonte MongoDB (mongodb_users)
--    O CPF é o user_key usado na tabela orders
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS users_mongo (
    uuid                 UUID           NOT NULL,
    user_id              INTEGER,
    cpf                  VARCHAR(20),   -- user_key em orders
    email                VARCHAR(200),
    phone_number         VARCHAR(30),
    city                 VARCHAR(100),
    country              VARCHAR(10),
    delivery_address     TEXT,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_users_mongo PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS idx_users_mongo_cpf     ON users_mongo (cpf);
CREATE INDEX IF NOT EXISTS idx_users_mongo_user_id ON users_mongo (user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 8. USERS — fonte MSSQL (mssql_users)
--    Perfil estendido: birthday, job, company — mesmo CPF do users_mongo
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS users_mssql (
    uuid                 UUID           NOT NULL,
    user_id              INTEGER,
    cpf                  VARCHAR(20),
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    phone_number         VARCHAR(30),
    birthday             DATE,
    job                  VARCHAR(200),
    company_name         VARCHAR(200),
    country              VARCHAR(10),
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_users_mssql PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS idx_users_mssql_cpf     ON users_mssql (cpf);
CREATE INDEX IF NOT EXISTS idx_users_mssql_user_id ON users_mssql (user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 9. RESTAURANTS (mysql_restaurants) — entidade
--    O CNPJ é o restaurant_key usado na tabela orders
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS restaurants (
    uuid                 UUID           NOT NULL,
    restaurant_id        INTEGER,
    cnpj                 VARCHAR(20),   -- restaurant_key em orders
    name                 VARCHAR(200),
    address              TEXT,
    city                 VARCHAR(100),
    country              VARCHAR(10),
    phone_number         VARCHAR(30),
    cuisine_type         VARCHAR(100),
    opening_time         TIME,
    closing_time         TIME,
    average_rating       NUMERIC(3,2),
    num_reviews          INTEGER,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_restaurants PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS idx_restaurants_cnpj          ON restaurants (cnpj);
CREATE INDEX IF NOT EXISTS idx_restaurants_restaurant_id ON restaurants (restaurant_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 10. DRIVERS (postgres_drivers) — entidade
--     driver_id (string) é o driver_key usado em orders e driver_shifts
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS drivers (
    uuid                 UUID           NOT NULL,
    driver_id            VARCHAR(20),   -- driver_key em orders
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    phone_number         VARCHAR(30),
    city                 VARCHAR(100),
    country              VARCHAR(10),
    date_birth           DATE,
    license_number       VARCHAR(50),
    vehicle_type         VARCHAR(50),
    vehicle_make         VARCHAR(50),
    vehicle_model        VARCHAR(50),
    vehicle_year         INTEGER,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_drivers PRIMARY KEY (uuid)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_drivers_driver_id ON drivers (driver_id);

-- ════════════════════════════════════════════════════════════════════════════
-- Publication do Debezium — exatamente as 10 tabelas Tier 1
-- Criada depois das tabelas: o Postgres exige que existam.
-- ════════════════════════════════════════════════════════════════════════════
CREATE PUBLICATION dbz_publication FOR TABLE
    payment_events,
    orders,
    order_items,
    driver_shifts,
    search_events,
    recommendations,
    users_mongo,
    users_mssql,
    restaurants,
    drivers;

-- ════════════════════════════════════════════════════════════════════════════
-- Seed — um registro representativo por domínio de entidade, para validar a
-- ingestão ponta a ponta sem depender da carga completa.
-- (O seed de `products` do projeto anterior saiu junto com o domínio Tier 2.)
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO restaurants (uuid, restaurant_id, cnpj, name, city, country, cuisine_type, average_rating, num_reviews, dt_current_timestamp)
VALUES ('11111111-1111-1111-1111-111111111111', 1, '00.000.000/0001-00', 'Seed Restaurant', 'São Paulo', 'BR', 'Brazilian', 4.5, 100, NOW())
ON CONFLICT DO NOTHING;

INSERT INTO drivers (uuid, driver_id, first_name, last_name, city, country, dt_current_timestamp)
VALUES ('22222222-2222-2222-2222-222222222222', 'DRV-00001', 'Seed', 'Driver', 'São Paulo', 'BR', NOW())
ON CONFLICT DO NOTHING;

INSERT INTO users_mongo (uuid, user_id, cpf, email, city, country, dt_current_timestamp)
VALUES ('33333333-3333-3333-3333-333333333333', 1, '000.000.000-00', 'seed@example.com', 'São Paulo', 'BR', NOW())
ON CONFLICT DO NOTHING;

INSERT INTO payment_events (event_id, payment_id, event, dt_current_timestamp)
VALUES (
    '44444444-4444-4444-4444-444444444444',
    '55555555-5555-5555-5555-555555555555',
    '{"event_name": "created", "timestamp": 1759687600000}',
    NOW()
) ON CONFLICT DO NOTHING;
