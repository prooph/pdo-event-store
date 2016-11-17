<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\PDO\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Interop\Container\ContainerInterface;
use PDO;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;

abstract class AbstractEventStoreProjectionFactory implements
    ProvidesDefaultOptions,
    RequiresConfig,
    RequiresConfigId,
    RequiresMandatoryOptions
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    protected $configId;

    /**
     * @var array
     */
    private $driverSchemeAliases = [
        'pdo_mysql'  => 'mysql',
        'pdo_pgsql'  => 'pgsql',
    ];

    private $driverSchemeSeparators = [
        'pdo_mysql'  => ';',
        'pdo_pgsql'  => ' ',
    ];

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     MySQLEventStoreProjection::class => [MySQLEventStoreProjectionFactory::class, 'service_name'],
     *     PostgresEventStoreProjection::class => [PostgresEventStoreProjectionFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments)
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }
        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId)
    {
        $this->configId = $configId;
    }

    public function dimensions()
    {
        return ['prooph', 'event_store_projection'];
    }

    protected function getEventStore(ContainerInterface $container, $config): EventStore
    {
        $eventStoreName = $config['event_store'];
        return MySQLEventStoreFactory::$eventStoreName($container);
    }

    protected function getConnection(ContainerInterface $container, $config): PDO
    {
        if (isset($config['connection_service'])) {
            $connection = $container->get($config['connection_service']);
        } else {
            $separator = $this->driverSchemeSeparators[$config['connection_options']['driver']];
            $dsn = $this->driverSchemeAliases[$config['connection_options']['driver']] . ':';
            $dsn .= 'host=' . $config['connection_options']['host'] . $separator;
            $dsn .= 'port=' . $config['connection_options']['port'] . $separator;
            $dsn .= 'dbname=' . $config['connection_options']['dbname'] . $separator;
            $dsn = rtrim($dsn);
            $user = $config['connection_options']['user'];
            $password = $config['connection_options']['password'];
            $connection = new PDO($dsn, $user, $password);
        }

        return $connection;
    }
}
