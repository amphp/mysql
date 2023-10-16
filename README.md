# amphp/mysql

AMPHP is a collection of event-driven libraries for PHP designed with fibers and concurrency in mind.
`amphp/mysql` is an asynchronous MySQL client.
The library allows to dynamically query a server with multiple MySQL connections concurrently.
The client transparently distributes these queries across a scalable pool of available connections and does so using 100% userland PHP; there are *no external extension dependencies* (e.g. `ext/mysqli`, `ext/pdo`, etc).

## Features

 - Exposes a non-blocking API for issuing multiple MySQL queries concurrently
 - Transparent connection pooling to overcome MySQL's fundamentally synchronous connection protocol
 - MySQL transfer encoding support (gzip, TLS encryption)
 - *Full* MySQL protocol support including *all*<sup>†</sup> available commands asynchronously

<sup>† As documented in [official Mysql Internals Manual](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)</sup>

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require amphp/mysql
```

This package requires PHP 8.1 or later.

## Usage

More extensive code examples reside in the [`examples`](examples) directory.

```php
$config = Amp\Mysql\ConnectionConfig::fromString(
    "host=127.0.0.1 user=username password=password db=test"
);

$pool = new Amp\Mysql\Pool($config);

$statement = $pool->prepare("SELECT * FROM table_name WHERE id = :id");
foreach ($statement->execute(['id' => 1337]) as $row) {
    // $row is an associative array of column values. e.g.: $row['column_name']
}
```

## Versioning

`amphp/mysql` follows the [semver](http://semver.org/) semantic versioning specification like all other `amphp` packages.

## Security

If you discover any security related issues, please email [`contact@amphp.org`](mailto:contact@amphp.org) instead of using the issue tracker.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
