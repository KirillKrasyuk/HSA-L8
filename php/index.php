<?php

class Connection {
    private static array $connections = [
        'db' => null
    ];

    protected function __construct() {}

    public static function getDBConnection(): PDO
    {
        if (!self::$connections['db']) {
            try {
                self::$connections['db'] = new PDO(
                    "mysql:host=mysql;dbname=db",
                    'admin',
                    'admin'
                );
                self::$connections['db']->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            } catch (\Exception $exception) {
                echo "Connection failed: " . $exception->getMessage();
            }
        }

        return self::$connections['db'];
    }
}

class Server {
    private array $dates = [];

    public function __construct(
        private PDO $connection
    ) {
    }

    public function initData()
    {
        $this->dropTable();
        $this->createTable();
        $this->createRecords('users');

        echo 'Init done' . PHP_EOL;
    }

    public function initMemoryData()
    {
        $this->dropMemoryTable();
        $this->createMemoryTable();
        $this->createRecords('users_memory');

        echo 'Init done' . PHP_EOL;
    }

    public function getDataWithoutIndex(): array
    {
        echo 'Run query without index...' . PHP_EOL;

        $query = $this->connection->prepare(
            sprintf(
                "SELECT * FROM users WHERE birthday = '%s';",
                date('Y-m-d', $this->getTimestamp()) // 1149061120
            )
        );
        $query->execute();

        return $query->fetchAll(PDO::FETCH_ASSOC);
    }

    public function getDataWithBtreeIndex(): array
    {
        echo 'Run query with btree index...' . PHP_EOL;

        $query = $this->connection->prepare(
            sprintf(
                "SELECT * FROM users WHERE birthday = '%s';",
                date('Y-m-d', $this->getTimestamp()) // 1149061120
            )
        );
        $query->execute();

        return $query->fetchAll(PDO::FETCH_ASSOC);
    }

    public function getDataWithHashIndex(): array
    {
        echo 'Run query with hash index...' . PHP_EOL;

        $query = $this->connection->prepare(
            sprintf(
                "SELECT * FROM users_memory WHERE birthday = '%s';",
                date('Y-m-d', $this->getTimestamp()) // 1149061120
            )
        );
        $query->execute();

        return $query->fetchAll(PDO::FETCH_ASSOC);
    }

    private function createRecords(string $table = 'users')
    {
        for ($i = 1; $i <= 40000000; $i++) {
            $this->dates[] = date('Y-m-d', $this->getTimestamp());

            if ($i % 10000 === 0) {
                $this->insertRecords($table);

                $this->dates = [];
            }
        }
    }

    private function insertRecords(string $table = 'users')
    {
        try {
            $sql = sprintf(
                "INSERT INTO %s (birthday) VALUES ('%s');",
                $table,
                implode("'), ('", $this->dates)
            );

            $query = $this->connection->prepare($sql);

            $query->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function dropTable()
    {
        try {
            $this->connection
                ->prepare("DROP TABLE IF EXISTS users;")
                ->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function dropMemoryTable()
    {
        try {
            $this->connection
                ->prepare("DROP TABLE IF EXISTS users_memory;")
                ->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function createTable()
    {
        try {
            $this->connection
                ->prepare("CREATE TABLE IF NOT EXISTS users (
                    id int auto_increment,
                    birthday date not null,
                    constraint data_pk primary key (id)
                ) ENGINE=InnoDB;")
                ->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function createMemoryTable()
    {
        try {
            $this->connection
                ->prepare("CREATE TABLE IF NOT EXISTS users_memory (
                    id int auto_increment,
                    birthday date not null,
                    constraint data_pk primary key (id)
                ) ENGINE=MEMORY;")
                ->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function createBtreeIndex(): void
    {
        try {
            $this->connection
                ->prepare(
                    "CREATE INDEX users_birthday ON users (birthday) USING BTREE;
                ")->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function createHashIndex(): void
    {
        try {
            $this->connection
                ->prepare(
                    "CREATE INDEX users_birthday ON users_memory (birthday) USING HASH;
                ")->execute();
        } catch (\Exception $exception) {
            echo "SQL error: " . $exception->getMessage() . PHP_EOL;
        }
    }

    private function getTimestamp(): string {
        return rand(0, time());
    }
}

$server = new Server(
    Connection::getDBConnection()
);

//$server->initData();
//$server->initMemoryData();

$startTime = microtime(true);

$result = [];
//$result = $server->getDataWithoutIndex();
//$result = $server->getDataWithBtreeIndex();
$result = $server->getDataWithHashIndex();

$endTime = microtime(true);
$duration = $endTime - $startTime;

echo sprintf('Results count %s. Duration %s sec', \count($result), $duration) . PHP_EOL;