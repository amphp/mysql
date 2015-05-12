Mysql [![Build Status](https://travis-ci.org/amphp/mysql.svg?branch=master)](https://travis-ci.org/amphp/mysql)
=====

`amp\mysql` is an asynchronous MySQL client built on the [amp concurrency framework][1]. The library exposes a Promise-based API to dynamically query multiple synchronous MySQL connections concurrently. The client transparently distributes these queries across a scalable pool of available connections and does so using 100% userland PHP; there are no external extension dependencies (e.g. `ext/mysqli`, `ext/pdo`, etc).

##### Features

 - Asynchronous API exposing full single-threaded concurrency;
 - Transparent connection pooling to overcome MySQL's fundamentally synchronous connection protocol;
 - MySQL transfer encoding support (gzip, TLS encryption);
 - Support for all MySQL commands<sup>†</sup>.

<small><sup>†</sup> As documented in [official Mysql Internals Manual][2]</small>

##### Project Goals

* Expose a non-blocking API for issuing multiple MySQL queries in parallel;
* Support the *full* MySQL protocol and *all* available commands asynchronously.

##### Installation

```bash
$ git clone https://github.com/amphp/mysql
$ cd mysql
$ composer.phar install
```

The relevant packagist lib is `amphp/mysql`.


Documentation & Examples
------------------------

More extensive code examples reside in the [`examples`](examples) directory.

##### Simple `SELECT` query

```php
$connection = new Amp\Mysql\Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS);
\Amp\wait($connection->connect());

$promise = $connection->query("SELECT 10"); // returns Promise returning ResultSet
$resultSet = \Amp\wait($promise);
$rows = \Amp\wait($resultSet->fetchAll());
var_dump($rows); // Array(1) { 0 => Array(1) { 0 => 10 } }

$connection->close();
```

Using a `Connection` object directly (as shown above) is klunky in terms of initialization. Instead we can use a `Pool` to automatically handle establishing the connection ...

##### Pooled connections

```php
$pool = new \Amp\Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS);

// We can use the pool immediately -- the connection state is transparent
$pool->query("...");
```

The `Pool` aggregates `Connection` instances as needed with configurable limits so we can get the most out of parallel queries.

##### Fetch modes on ResultSet

**Note**: Methods on ResultSet class generally return a Promise

 - Fetching all rows at once (in a big array)
  - fetchRows
  - fetchObjects
  - fetchAll<sup>†</sup>
 - Fetching only the next row
  - fetchRow
  - fetchObject
  - fetch<sup>†</sup>

<small><sup>†</sup>`fetch()` and `fetchAll()` methods are combined fetchRow(s)/fetchObject(s), means, they contain the integer _and_ string keys</small>

In case of a multi-ResultSet, use the `next()` method to get the next ResultSet.

ResultSet also has `getFields` method which returns an array of arrays with column data and `rowCount()` to fetch the number of rows.

##### prepare()'d statements

Pool and Connection classes provides a `prepare()` command for prepared statement handling. Placeholders are `:NAME` or `?` (numeric index).<sup>†</sup>

```php
$stmt = \Amp\wait($pool->prepare("SELECT * FROM table WHERE column = :value"));
$resultSet = \Amp\wait($stmt->execute(["value" => "a value"]));
$rows = \Amp\wait($resultSet->fetchAll());
```
Or short, to immediately execute the prepared statement:
```php
$resultSet = \Amp\wait($pool->prepare("SELECT * FROM table WHERE column = ?", ["a value"]));
$rows = \Amp\wait($resultSet->fetchAll());
```

**Note**: Stmt class also provides `getFields()` (returning Promise which will hold an array of arrays with column info), `bind($paramIdentifier, $value)` (binds a parameter, it's then not necessary to pass it in `execute()`) and `reset()` (to reset values bound via `bind()`) methods.

<small><sup>†</sup> yes, the MySQL protocol does not support `:NAME` placeholders; they are internally replaced by `?` characters</small>

##### Other methods than query() and prepare()

Pool and Connection classes also have several methods that directly mirror some text protocol functionality:

 - `close()`
 - `useDb($db)`<sup>†</sup>
 - `listAllFields($table, $like = "%")` (Promise which will hold an array of arrays with column info)
 - `listFields($table, $like = "%")` (Promise which will hold an array `[$column, $promise]` where `$column` is an array with column info and $promise will hold the next column info etc.)
 - `createDatabase($db)`
 - `refresh($subcommand)` (See `Connection::REFRESH_*` constants)
 - `shutdown()`
 - `processInfo()` (Promise which will hold a ResultSet)
 - `killProcess()`
 - `debugStdout()`
 - `ping()`
 - ~~`changeUser($user, $pass, $db = null)`~~<sup>†</sup> (currently not available)
 - `resetConnection()`<sup>†</sup>

<small><sup>†</sup> All these methods are solely present in Connection class, but not in Pool, as they don't make sense there.</small>

##### Use a separate Connection class

Pool class also provides a `getConnection()` method which unmaps a Connection object from Pool and returns it, so that you can execute stateful operations on it. (E.g. SET commands which are specific for one connection and which should not be used in the general Pool). Only disadvantage here is that no two operations can be then executed at the same time as MySQL connections one allow sequential command processing.

**Attention**: When using a separate Connection class, it is ***very* important** to close the Connection yourself via `$connection->close()`, because our Connection objects are still referenced by the Reactor.


  [1]: https://github.com/amphp/amp
  [2]: https://dev.mysql.com/doc/internals/en/client-server-protocol.html
