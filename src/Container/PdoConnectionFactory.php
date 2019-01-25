<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2019 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use PDO;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Psr\Container\ContainerInterface;

class PdoConnectionFactory implements ProvidesDefaultOptions, RequiresConfigId, RequiresMandatoryOptions
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    private $configId;

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     PDO::class => [PdoConnectionFactory::class, 'mysql'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): PDO
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                \sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }

        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): PDO
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        return new PDO(
            $this->buildConnectionDns($config),
            $config['user'],
            $config['password']
        );
    }

    public function dimensions(): iterable
    {
        return [
            'prooph',
            'pdo_connection',
        ];
    }

    public function defaultOptions(): iterable
    {
        return [
            'host' => '127.0.0.1',
            'dbname' => 'event_store',
            'charset' => 'utf8',
        ];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'schema',
            'user',
            'password',
            'port',
        ];
    }

    private function buildConnectionDns(array $params): string
    {
        $dsn = $params['schema'] . ':';

        if ($params['host'] !== '') {
            $dsn .= 'host=' . $params['host'] . ';';
        }

        if ($params['port'] !== '') {
            $dsn .= 'port=' . $params['port'] . ';';
        }

        $dsn .= 'dbname=' . $params['dbname'] . ';';

        if ('mysql' === $params['schema']) {
            $dsn .= 'charset=' . $params['charset'] . ';';
        } elseif ('pgsql' === $params['schema']) {
            $dsn .= "options='--client_encoding=".$params['charset']."'";
        }

        return $dsn;
    }
}
