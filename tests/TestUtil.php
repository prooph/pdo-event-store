<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo;

use PDO;

abstract class TestUtil
{
    /**
     * List of URL schemes from a database URL and their mappings to driver.
     */
    private static $driverSchemeAliases = [
        'pdo_mysql' => 'mysql',
        'pdo_pgsql' => 'pgsql',
    ];

    private static $driverSchemeSeparators = [
        'pdo_mysql' => ';',
        'pdo_pgsql' => ' ',
    ];

    /**
     * @var PDO
     */
    private static $connection;

    public static function getConnection(): PDO
    {
        if (! isset(self::$connection)) {
            $connectionParams = self::getConnectionParams();
            $separator = self::$driverSchemeSeparators[$connectionParams['driver']];
            $dsn = self::$driverSchemeAliases[$connectionParams['driver']] . ':';
            $dsn .= 'host=' . $connectionParams['host'] . $separator;
            $dsn .= 'port=' . $connectionParams['port'] . $separator;
            $dsn .= 'dbname=' . $connectionParams['dbname'] . $separator;
            $dsn .= self::getCharsetValue($connectionParams['charset'], $connectionParams['driver']) . $separator;
            $dsn = rtrim($dsn);

            $retries = 10; // keep trying for 10 seconds, should be enough
            while (null === self::$connection && $retries > 0) {
                try {
                    self::$connection = new PDO($dsn, $connectionParams['user'], $connectionParams['password'], $connectionParams['options']);
                } catch (\PDOException $e) {
                    if (2002 !== $e->getCode()) {
                        throw $e;
                    }

                    $retries--;
                    sleep(1);
                }
            }
        }

        try {
            self::$connection->rollBack();
        } catch (\PDOException $e) {
            // ignore
        }

        return self::$connection;
    }

    public static function getDatabaseName(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return getenv('DB_NAME');
    }

    public static function getDatabaseDriver(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return getenv('DB_DRIVER');
    }

    public static function getDatabaseVendor(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return explode('_', getenv('DB'))[0];
    }

    public static function getConnectionParams(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    public static function initDefaultDatabaseTables(PDO $connection): void
    {
        $vendor = self::getDatabaseVendor();

        $connection->exec('DROP TABLE IF EXISTS event_streams');
        $connection->exec(file_get_contents(__DIR__.'/../scripts/' . $vendor . '/01_event_streams_table.sql'));
        $connection->exec('DROP TABLE IF EXISTS projections');
        $connection->exec(file_get_contents(__DIR__.'/../scripts/' . $vendor . '/02_projections_table.sql'));
    }

    public static function initCustomDatabaseTables(PDO $connection): void
    {
        $vendor = self::getDatabaseVendor();

        $connection->exec('DROP TABLE IF EXISTS event_streams');
        $connection->exec(file_get_contents(__DIR__.'/Assets/scripts/' . $vendor . '/01_event_streams_table.sql'));
        $connection->exec('DROP TABLE IF EXISTS projections');
        $connection->exec(file_get_contents(__DIR__.'/Assets/scripts/' . $vendor . '/02_projections_table.sql'));
    }

    private static function hasRequiredConnectionParams(): bool
    {
        $env = getenv();

        return isset(
            $env['DB'],
            $env['DB_DRIVER'],
            $env['DB_USERNAME'],
            $env['DB_PASSWORD'],
            $env['DB_HOST'],
            $env['DB_NAME'],
            $env['DB_PORT'],
            $env['DB_CHARSET']
        );
    }

    private static function getSpecifiedConnectionParams(): array
    {
        return [
            'driver' => getenv('DB_DRIVER'),
            'user' => getenv('DB_USERNAME'),
            'password' => getenv('DB_PASSWORD'),
            'host' => getenv('DB_HOST'),
            'dbname' => getenv('DB_NAME'),
            'port' => getenv('DB_PORT'),
            'charset' => getenv('DB_CHARSET'),
            'options' => [PDO::ATTR_ERRMODE => (int) getenv('DB_ATTR_ERRMODE')],
        ];
    }

    private static function getCharsetValue(string $charset, string $driver): string
    {
        if ('pdo_pgsql' === $driver) {
            return "options='--client_encoding=$charset'";
        }

        return "charset=$charset";
    }
}
