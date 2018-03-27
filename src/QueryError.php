<?php

namespace Amp\Mysql;

class QueryError extends \Error {
    protected $query = "";

    public function __construct(string $message, string $query = "", \Throwable $previous = null) {
        if ($query != "") {
            $this->query = $query;
        }
        parent::__construct($message, 0, $previous);
    }

    final public function getQuery(): string {
        return $this->query;
    }

    public function __toString(): string {
        if ($this->query == "") {
            return parent::__toString();
        }

        $msg = $this->message;
        $this->message .= "\nCurrent query was {$this->query}";
        $str = parent::__toString();
        $this->message = $msg;
        return $str;
    }
}
