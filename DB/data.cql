-- =========================
-- KEYSPACE
-- =========================
CREATE KEYSPACE IF NOT EXISTS notification
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE notification;

-- =========================
-- USERS TABLE
-- =========================
CREATE TABLE IF NOT EXISTS users (
    id text PRIMARY KEY,
    name text,
    email text,
    device_token text
);

-- =========================
-- NOTIFICATION TEMPLATES
-- =========================
CREATE TABLE IF NOT EXISTS notification_templates (
    event_type text PRIMARY KEY,
    template text
);

-- =========================
-- INSERT USERS
-- =========================
INSERT INTO users (id, name, email, device_token)
VALUES ('u1', 'Alice', 'alice@example.com', 'fcm_token_alice');

INSERT INTO users (id, name, email, device_token)
VALUES ('u2', 'Bob', 'bob@example.com', 'apn_token_bob');

INSERT INTO users (id, name, email, device_token)
VALUES ('u3', 'Charlie', 'charlie@example.com', 'fcm_token_charlie');

-- =========================
-- INSERT TEMPLATES
-- =========================
INSERT INTO notification_templates (event_type, template)
VALUES (
    'PAYMENT_SUCCESS',
    'Hi {name}, your payment was successful. Thank you!'
);

INSERT INTO notification_templates (event_type, template)
VALUES (
    'COURSE_COMPLETED',
    'Congratulations {name}! You have successfully completed the course.'
);

INSERT INTO notification_templates (event_type, template)
VALUES (
    'ASSIGNMENT_SUBMITTED',
    'Hi {name}, your assignment has been submitted successfully.'
);
