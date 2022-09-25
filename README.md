<p align="center">
<a href="https://amphp.org/mysql"><img src="https://raw.githubusercontent.com/amphp/logo/master/repos/mysql.png?v=12-07-2017" alt="mysql"/></a>
</p>

<p align="center">
<a href="https://travis-ci.org/amphp/mysql"><img src="https://img.shields.io/travis/amphp/mysql/master.svg?style=flat-square" alt="Build Status"/></a>
<a href="https://coveralls.io/github/amphp/mysql?branch=master"><img src="https://img.shields.io/coveralls/amphp/mysql/master.svg?style=flat-square" alt="Code Coverage"/></a>
<a href="https://github.com/amphp/mysql/releases"><img src="https://img.shields.io/github/release/amphp/mysql.svg?style=flat-square" alt="Release"/></a>
<a href="https://github.com/amphp/mysql/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License"/></a>
</p>

<p align="center"><strong>Async MySQL client built with <a href="https://amphp.org/">Amp</a>.</strong></p>

<hr/>

`amphp/mysql` is an asynchronous MySQL client built on the [Amp concurrency framework](https://amphp.org/). The library exposes a Promise-based API to dynamically query multiple MySQL connections concurrently. The client transparently distributes these queries across a scalable pool of available connections and does so using 100% userland PHP; there are *no external extension dependencies* (e.g. `ext/mysqli`, `ext/pdo`, etc).

#### Features

 - Asynchronous API exposing full single-threaded concurrency
 - Transparent connection pooling to overcome MySQL's fundamentally synchronous connection protocol
 - MySQL transfer encoding support (gzip, TLS encryption)
 - Support for all MySQL commands<sup>†</sup>

<sup>† As documented in [official Mysql Internals Manual](https://dev.mysql.com/doc/internals/en/client-server-protocol.html)</sup>

#### Project Goals

* Expose a non-blocking API for issuing multiple MySQL queries in parallel
* Support the *full* MySQL protocol and *all* available commands asynchronously

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require amphp/mysql
```

## Requirements

* PHP 7.1+
* [Amp framework](https://github.com/amphp/amp) (installed via composer)

## Documentation & Examples

More extensive code examples reside in the [`examples`](examples) directory.

```php
Amp\Loop::run(function() {
    $config = Amp\Mysql\ConnectionConfig::fromString(
        "host=127.0.0.1 user=username password=password db=test"
    );
    
    /** @var \Amp\Mysql\Pool $pool */
    $pool = Amp\Mysql\pool($config);
    
    /** @var \Amp\Mysql\Statement $statement */
    $statement = yield $pool->prepare("SELECT * FROM table_name WHERE id = :id");
    
    /** @var \Amp\Mysql\ResultSet $result */
    $result = yield $statement->execute(['id' => 1337]);
    while (yield $result->advance()) {
        $row = $result->getCurrent();
        // $row is an associative array of column values. e.g.: $row['column_name']
    }
});
```
## Versioning

`amphp/mysql` follows the [semver](http://semver.org/) semantic versioning specification like all other `amphp` packages.

## Security

If you discover any security related issues, please email [`contact@amphp.org`](mailto:contact@amphp.org) instead of using the issue tracker.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
