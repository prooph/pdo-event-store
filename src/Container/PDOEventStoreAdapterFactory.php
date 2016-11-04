<?php
/**
 * This file is part of the prooph/event-store-pdo-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Prooph\EventStore\Adapter\PDO\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresConfigId;
use Interop\Container\ContainerInterface;
use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Exception\InvalidArgumentException;
use Prooph\EventStore\Adapter\PDO\IndexingStrategy\MySQLOneStreamPerAggregate;
use Prooph\EventStore\Adapter\PDO\PDOEventStoreAdapter;
use Prooph\EventStore\Adapter\PDO\TableNameGeneratorStrategy\Sha1;

final class PDOEventStoreAdapterFactory implements RequiresConfig, RequiresConfigId, ProvidesDefaultOptions
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    private $configId;

    /**
     * @var array
     */
    private $driverSchemeAliases = [
        'pdo_mysql'  => 'mysql',
        'pdo_pgsql'  => 'postgres',
        'pdo_sqlite' => 'sqlite',
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
     *     PDOEventStoreAdapter::class => [PDOEventStoreAdapterFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): PDOEventStoreAdapter
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }
        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): PDOEventStoreAdapter
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId)['adapter']['options'];

        if (isset($config['connection_service'])) {
            $connection = $container->get($config['connection_service']);
        } else {
            $dsn = $this->driverSchemeAliases[$config['connection_options']['driver']] . ':'
                . 'host=' . $config['connection_options']['host'] . ';'
                . 'port=' . $config['connection_options']['port'] . ';'
                . 'dbname=' . $config['connection_options']['dbname'];
            $user = $config['connection_options']['user'];
            $password = $config['connection_options']['password'];
            $connection = new PDO($dsn, $user, $password);
        }

        $messageFactory = $container->has(MessageFactory::class)
            ? $container->get(MessageFactory::class)
            : new FQCNMessageFactory();

        $messageConverter = $container->has(MessageConverter::class)
            ? $container->get(MessageConverter::class)
            : new NoOpMessageConverter();

        $indexingStrategy = $container->get($config['indexing_strategy']);

        $tableNameGeneratorStrategy = $container->get($config['table_name_generator_strategy']);

        return new PDOEventStoreAdapter(
            $messageFactory,
            $messageConverter,
            $connection,
            $indexingStrategy,
            $tableNameGeneratorStrategy,
            $config['load_batch_size'],
            $config['event_streams_table']
        );
    }

    public function dimensions(): array
    {
        return ['prooph', 'event_store'];
    }

    public function defaultOptions(): array
    {
        return [
            'adapter' => [
                'options' => [
                    'connection_options' => [
                        'driver' => 'pdo_mysql',
                        'user' => 'root',
                        'password' => '',
                        'host' => '127.0.0.1',
                        'dbname' => 'event_store',
                        'port' => 3306,
                    ],
                    'indexing_strategy' => MySQLOneStreamPerAggregate::class,
                    'table_name_generator_strategy' => Sha1::class,
                    'load_batch_size' => 1000,
                    'event_streams_table' => 'event_streams',
                ]
            ]
        ];
    }
}
