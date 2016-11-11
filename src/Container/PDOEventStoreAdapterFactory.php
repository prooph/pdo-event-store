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
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\PDOEventStoreAdapter;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;

final class PDOEventStoreAdapterFactory implements
    ProvidesDefaultOptions,
    RequiresConfig,
    RequiresConfigId,
    RequiresMandatoryOptions
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

        $messageFactory = $container->has(MessageFactory::class)
            ? $container->get(MessageFactory::class)
            : new FQCNMessageFactory();

        $messageConverter = $container->has(MessageConverter::class)
            ? $container->get(MessageConverter::class)
            : new NoOpMessageConverter();

        $jsonQuerier = $container->get($config['json_querier']);

        $indexingStrategy = $container->get($config['indexing_strategy']);

        $tableNameGeneratorStrategy = $container->get($config['table_name_generator_strategy']);

        return new PDOEventStoreAdapter(
            $messageFactory,
            $messageConverter,
            $connection,
            $jsonQuerier,
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
                    'table_name_generator_strategy' => Sha1::class,
                    'load_batch_size' => 1000,
                    'event_streams_table' => 'event_streams',
                ],
            ],
        ];
    }

    public function mandatoryOptions(): array
    {
        return [
            'adapter' => [
                'options' => [
                    'json_querier',
                    'indexing_strategy',
                ],
            ],
        ];
    }
}
