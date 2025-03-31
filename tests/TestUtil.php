<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2025 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2025 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
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

    public static function getConnection($sharedConnection = true): PDO
    {
        if (! isset(self::$connection) || ! $sharedConnection) {
            $connectionParams = self::getConnectionParams();
            $separator = self::$driverSchemeSeparators[$connectionParams['driver']];
            $dsn = self::$driverSchemeAliases[$connectionParams['driver']] . ':';
            $dsn .= 'host=' . $connectionParams['host'] . $separator;
            $dsn .= 'port=' . $connectionParams['port'] . $separator;
            $dsn .= 'dbname=' . $connectionParams['dbname'] . $separator;
            $dsn .= self::getCharsetValue($connectionParams['charset'], $connectionParams['driver']) . $separator;
            $dsn = \rtrim($dsn);

            if (! $sharedConnection) {
                return new PDO($dsn, $connectionParams['user'], $connectionParams['password'], $connectionParams['options']);
            }

            $retries = 10; // keep trying for 10 seconds, should be enough
            while (null === self::$connection && $retries > 0) {
                try {
                    self::$connection = new PDO($dsn, $connectionParams['user'], $connectionParams['password'], $connectionParams['options']);
                } catch (\PDOException $e) {
                    if (2002 !== $e->getCode()) {
                        throw $e;
                    }

                    $retries--;
                    \sleep(1);
                }
            }
        }

        if (! self::$connection) {
            print "db connection could not be established. aborting...\n";
            exit(1);
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

        return \getenv('DB_NAME');
    }

    public static function getDatabaseDriver(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return \getenv('DB_DRIVER');
    }

    public static function getDatabaseVendor(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return \explode('_', \getenv('DB'))[0];
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

        $connection->exec(\file_get_contents(__DIR__ . '/../scripts/' . $vendor . '/01_event_streams_table.sql'));
        $connection->exec(\file_get_contents(__DIR__ . '/../scripts/' . $vendor . '/02_projections_table.sql'));
    }

    public static function initCustomDatabaseTables(PDO $connection): void
    {
        $vendor = self::getDatabaseVendor();

        $connection->exec(\file_get_contents(__DIR__ . '/Assets/scripts/' . $vendor . '/01_event_streams_table.sql'));
        $connection->exec(\file_get_contents(__DIR__ . '/Assets/scripts/' . $vendor . '/02_projections_table.sql'));
    }

    public static function initCustomSchemaDatabaseTables(PDO $connection): void
    {
        $vendor = self::getDatabaseVendor();

        $connection->exec(\file_get_contents(__DIR__ . '/Assets/scripts/' . $vendor . '/01_custom_event_streams_table.sql'));
        $connection->exec(\file_get_contents(__DIR__ . '/Assets/scripts/' . $vendor . '/02_custom_projections_table.sql'));
    }

    public static function tearDownDatabase(): void
    {
        $connection = self::getConnection();
        $vendor = self::getDatabaseVendor();
        switch ($vendor) {
            case 'postgres':
                $statement = $connection->prepare('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';');
                $connection->exec('DROP SCHEMA IF EXISTS prooph CASCADE');

                break;
            default:
                $statement = $connection->prepare('SHOW TABLES');

                break;
        }

        $statement->execute();
        $tables = $statement->fetchAll(PDO::FETCH_COLUMN);

        foreach ($tables as $table) {
            switch ($vendor) {
                case 'postgres':
                    $connection->exec(\sprintf('DROP TABLE "%s";', $table));

                    break;
                default:
                    $connection->exec(\sprintf('DROP TABLE `%s`;', $table));

                    break;
            }
        }
    }

    public static function getProjectionLockedUntilFromDefaultProjectionsTable(PDO $connection, string $projectionName): ?\DateTimeImmutable
    {
        $vendor = self::getDatabaseVendor();

        $projectionsTable = '`projections`';

        if ($vendor === 'postgres') {
            $projectionsTable = '"projections"';
        }

        $sql = "SELECT locked_until FROM $projectionsTable WHERE name = ?";

        $statement = $connection->prepare($sql);

        $statement->execute([$projectionName]);

        $lockedUntil = $statement->fetchColumn();

        if ($lockedUntil) {
            return \DateTimeImmutable::createFromFormat('Y-m-d\TH:i:s.u', $lockedUntil, new \DateTimeZone('UTC'));
        }

        return null;
    }

    public static function subMilliseconds(\DateTimeImmutable $time, int $ms): \DateTimeImmutable
    {
        //Create a 0 interval
        $interval = new \DateInterval('PT0S');
        //and manually add split seconds
        $interval->f = $ms / 1000;

        return $time->sub($interval);
    }

    private static function hasRequiredConnectionParams(): bool
    {
        $env = \getenv();

        return isset(
            $env['DB'],
            $env['DB_DRIVER'],
            $env['DB_USERNAME'],
            $env['DB_HOST'],
            $env['DB_NAME'],
            $env['DB_PORT'],
            $env['DB_CHARSET']
        );
    }

    private static function getSpecifiedConnectionParams(): array
    {
        return [
            'driver' => \getenv('DB_DRIVER'),
            'user' => \getenv('DB_USERNAME'),
            'password' => false !== \getenv('DB_PASSWORD') ? \getenv('DB_PASSWORD') : '',
            'host' => \getenv('DB_HOST'),
            'dbname' => \getenv('DB_NAME'),
            'port' => \getenv('DB_PORT'),
            'charset' => \getenv('DB_CHARSET'),
            'options' => [PDO::ATTR_ERRMODE => (int) \getenv('DB_ATTR_ERRMODE')],
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
