<?php

require 'support/bootstrap.php';
require 'support/generic-table.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $db = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".DB_HOST.";user=".DB_USER.";pass=".DB_PASS.";db=".DB_NAME));

    yield $db->query("DROP TABLE IF EXISTS amp_json");

    yield $db->query("CREATE TABLE IF NOT EXISTS amp_json (a TEXT, b JSON)");

    /** @var \Amp\Mysql\Statement $statement */
    $statement = yield $db->prepare("INSERT INTO amp_json (a, b) VALUES (?, cast(? as json))");
    $json = '{"id": "evt_1FOZLq2dR4q4lysGBlnAxReO", "data": {"object": {"id": "po_1FODXN2dR4q4lysGUftc642s", "type": "bank_account", "amount": 75417, "method": "standard", "object": "payout", "status": "paid", "created": 1569806333, "currency": "usd", "livemode": true, "metadata": [], "automatic": true, "description": "STRIPE PAYOUT", "destination": "ba_1ATw1g2dR4q4lysGqe7POQQB", "source_type": "card", "arrival_date": 1569888000, "failure_code": null, "failure_message": null, "balance_transaction": "txn_1FODXN2dR4q4lysGxJg5ir6s", "statement_descriptor": null, "failure_balance_transaction": null}}, "type": "payout.paid", "object": "event", "created": 1569890186, "request": {"id": null, "idempotency_key": null}, "livemode": true, "api_version": "2019-08-14", "pending_webhooks": 1}';
    yield $statement->execute([$json, $json]);

    print_r(yield \Amp\Iterator\toArray(yield $db->query('select * from amp_json')));

    $db->close();
});
