FROM composer AS composer

FROM php:8.3-alpine

RUN set -eux; \
	apk add --no-cache --virtual .build-deps \
	  $PHPIZE_DEPS \
      linux-headers \
	  libzip-dev \
      postgresql-dev \
	  zlib-dev \
	; \
	\
    pecl install \
      xdebug-3.4.1 \
    \
    docker-php-ext-configure zip; \
    docker-php-ext-configure pcntl --enable-pcntl; \
    docker-php-ext-install -j$(nproc) \
      pcntl \
      pdo_mysql \
      pdo_pgsql \
      zip \
    ; \
    docker-php-ext-enable \
      pcntl \
      xdebug \
      zip \
    ; \
    \
	\
	runDeps="$( \
		scanelf --needed --nobanner --format '%n#p' --recursive /usr/local/lib/php/extensions \
			| tr ',' '\n' \
			| sort -u \
			| awk 'system("[ -e /usr/local/lib/" $1 " ]") == 0 { next } { print "so:" $1 }' \
	)"; \
	apk add --no-cache --virtual .phpexts-rundeps $runDeps; \
    \
    apk del .build-deps

COPY --from=composer /usr/bin/composer /usr/bin/composer

WORKDIR /app
