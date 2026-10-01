# PHP image for the example apps: FrankenPHP with pdo_pgsql and pdo_mysql for the shop
# and rdkafka for the consumer and the tests that read Kafka.
FROM dunglas/frankenphp:1-php8.4
RUN install-php-extensions pdo_pgsql pdo_mysql rdkafka zip \
    # Let real environment variables reach $_SERVER, so Symfony prefers them over .env.
    && echo 'variables_order = "EGPCS"' > "$PHP_INI_DIR/conf.d/zz-examples.ini"
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer
