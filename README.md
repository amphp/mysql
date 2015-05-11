Mysql [![Build Status](https://travis-ci.org/amphp/mysql.svg?branch=master)](https://travis-ci.org/amphp/mysql)
=====

Amp\Mysql is a mysql client built on the [amp concurrency framework][1]. It provides an asynchronous interface how to communicate with multiple synchronous mysql connections, distributing queries as efficiently as possible over the separate connections, being transparently usable through a single Pool of Connections. The library has no dependency on internal mysql extensions, but communicates directly with the mysql server.

##### Features

 - Asynchronous interface for full single-threaded concurrency
 - Pools multiple connections (mysql protocol being fundamentally synchronous)
 - Implements various sorts of mysql transfer (like gzip or TLS)
 - Fully featured and supports all commands<sup>†</sup>

<small><sup>†</sup> As far as documented in [official Mysql Internals Manual][2]</small>

##### Project Goals

* Providing a fast possibility to issue multiple commands in parallel
* Full mysql protocol support with asynchronous fetching possibilities

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

##### Simple mysql query

Just issue a simple SELECT query:

```php
$connection = new \Mysql\Connection("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS);
\Amp\wait($connection->connect());

$promise = $connection->query("SELECT 10"); // returns Promise returning ResultSet
$resultSet = \Amp\wait($promise);
$rows = \Amp\wait($resultSet->fetchAll());
var_dump($rows); // Array(1) { 0 => Array(1) { 0 => 10 } }

$connection->close();
```

That ends up a bit complicated for initialization… So, rather use a Pool, which does connection establishing for us...

##### Pooled connections

```php
$pool = new \Mysql\Pool("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS);

$pool->query("..."); // we immediately can just use it
```

The Pool class pools a lot of Connections [as much as needed, but not more], so that we can have a maximum of simultaneous operations happening at the same time.

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