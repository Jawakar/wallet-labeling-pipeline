create_wallet_labels_postgres_table = """
CREATE TABLE IF NOT EXISTS wallet_labels (
    address VARCHAR(42) PRIMARY KEY,
    label VARCHAR(255) NOT NULL,
    category VARCHAR(255),
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

update_wallet_labels_postgres_table = """
INSERT INTO wallet_labels (address, label, category, last_updated_at)
VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
ON CONFLICT (address)
DO UPDATE SET
    label = EXCLUDED.label,
    category = EXCLUDED.category,
    last_updated_at = EXCLUDED.last_updated_at;
"""