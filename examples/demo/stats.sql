-- What the dashboard reads, set up after the shop has run its migrations. Safe to run
-- again: compose runs it on every start.
--
-- shop_paid is the shop's side of the check: a trigger adds every payment to it in the
-- transaction that marks the order paid, so it is exact and cheap to read. The consumer
-- keeps its own totals in customer_spend from the OrderPaid events it read from Kafka.
-- The two must agree for every customer, give or take the payments still on their way.
-- The trigger is only here for the dashboard; it is not part of the outbox pattern.

BEGIN;
-- Payments stop while the trigger goes in, so none is counted twice or missed.
LOCK TABLE orders IN SHARE ROW EXCLUSIVE MODE;

CREATE SCHEMA IF NOT EXISTS demo_stats;

CREATE TABLE IF NOT EXISTS demo_stats.shop_paid (
    customer     text PRIMARY KEY,
    total        bigint NOT NULL,
    orders       bigint NOT NULL,
    last_paid_at timestamptz NOT NULL
);

CREATE OR REPLACE FUNCTION demo_stats.add_payment() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO demo_stats.shop_paid AS p (customer, total, orders, last_paid_at)
    VALUES (NEW.customer, NEW.total, 1, now())
    ON CONFLICT (customer) DO UPDATE
        SET total = p.total + excluded.total,
            orders = p.orders + 1,
            last_paid_at = excluded.last_paid_at;
    RETURN NEW;
END $$;

-- Payments made before the trigger existed, counted on the first run only.
INSERT INTO demo_stats.shop_paid (customer, total, orders, last_paid_at)
SELECT customer, sum(total), count(*), max(updated_at)
FROM orders
WHERE status = 'paid'
  AND NOT EXISTS (SELECT FROM pg_trigger WHERE tgname = 'orders_paid')
GROUP BY customer;

CREATE OR REPLACE TRIGGER orders_paid
    AFTER UPDATE OF status ON orders
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM 'paid' AND NEW.status = 'paid')
    EXECUTE FUNCTION demo_stats.add_payment();
COMMIT;

-- One row: the money on both sides and the number of customers where they disagree.
-- A customer counts only if their last payment is more than a minute old, so events
-- still between the table and the consumer do not show up as a difference.
CREATE OR REPLACE VIEW demo_stats.money AS
SELECT
    (SELECT coalesce(sum(total), 0) FROM demo_stats.shop_paid) AS paid_in_shop,
    (SELECT coalesce(sum(total), 0) FROM customer_spend) AS counted_by_consumer,
    (SELECT coalesce(sum(orders), 0) FROM demo_stats.shop_paid) AS paid_orders,
    (SELECT count(*)
       FROM demo_stats.shop_paid p
       FULL JOIN customer_spend c USING (customer)
      WHERE coalesce(p.last_paid_at, '-infinity') < now() - interval '1 minute'
        AND p.total IS DISTINCT FROM c.total) AS customers_that_disagree;

-- Grafana logs in as grafana_ro. It sees the view and nothing else, and a query may run
-- for two seconds at most: the dashboard is public, and so is what it can query.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN;
    END IF;
END $$;
ALTER ROLE grafana_ro PASSWORD :'grafana_password';
ALTER ROLE grafana_ro SET statement_timeout = '2s';
ALTER ROLE grafana_ro CONNECTION LIMIT 5;
REVOKE ALL ON SCHEMA public FROM grafana_ro;
GRANT USAGE ON SCHEMA demo_stats TO grafana_ro;
GRANT SELECT ON demo_stats.money TO grafana_ro;
