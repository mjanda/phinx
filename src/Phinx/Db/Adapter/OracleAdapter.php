<?php
/**
 * Phinx
 *
 * (The MIT license)
 * Copyright (c) 2015 Rob Morgan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated * documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 * @package    Phinx
 * @subpackage Phinx\Db\Adapter
 */
namespace Phinx\Db\Adapter;

use Phinx\Db\Table;
use Phinx\Db\Table\Column;
use Phinx\Db\Table\Index;
use Phinx\Db\Table\ForeignKey;
use Phinx\Migration\MigrationInterface;

/**
 * Phinx SqlServer Adapter.
 *
 * @author Rob Morgan <robbym@gmail.com>
 */
class OracleAdapter extends PdoAdapter implements AdapterInterface
{
    protected $autocommit = true;

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        if (null === $this->connection) {
            if (!extension_loaded('oci8')) {
                throw new \RuntimeException('You need to enable the oci8 extension for Phinx to run properly.');
            }

            $db = null;
            $options = $this->getOptions();

            // set autocommit
            $this->resetAutocommit();

            // default oracle port
            $port = isset($options['port']) ? $options['port'] : 1521;

            $db = @oci_new_connect($options['user'], $options['pass'], $options['host'].':'.$port.'/'.$options['sid'], $options['charset']);

            if (!$db) {
                $err = oci_error();
                throw new \RuntimeException($err['message'], $err['code']);
            }

            $this->setOciConnection($db);

            $this->query("ALTER session SET NLS_TIMESTAMP_TZ_FORMAT = 'yyyy-mm-dd hh24:mi:ss' NLS_SORT=BINARY_CI");
        }
    }

    /**
     * Save current DB connection
     *
     * @param resource $connection OCI8 resource
     */
    public function setOciConnection($connection)
    {
        $this->connection = $connection;

        // Create the schema table if it doesn't already exist
        if (!$this->hasSchemaTable()) {
            $this->execute('CREATE TABLE '.$this->getSchemaTableName().' (
                                VERSION NUMBER (19, 0),
                                MIGRATION_NAME VARCHAR2(100),
                                START_TIME TIMESTAMP WITH TIME ZONE,
                                END_TIME TIMESTAMP WITH TIME ZONE
                            )');
        }

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        $this->connection = null;
    }

    /**
     * Executes SQL using OCI8 driver
     */
    public function execute($sql)
    {
        return $this->query($sql);
    }

    /**
     * Executes SQL using OCI8 driver
     */
    public function query($sql)
    {
        $res = oci_parse($this->getConnection(), $sql);
        @oci_execute($res, $this->autocommit ? OCI_COMMIT_ON_SUCCESS : OCI_DEFAULT);

        $err = oci_error($res);
        if ($err) {
            throw new \RuntimeException($err['message'].' ['.$sql.']', $err['code']);
        }
        return $res;
    }

    /**
     * Throws not implemented exception so we don't need to repeat it everywhere
     * @return void
     * @throws RuntimeException
     */
    private function notImplementedYet()
    {
        throw new \RuntimeException("Not implemented - Oracle adapter supports only pure SQL migrations");
    }

    /**
     * {@inheritdoc}
     */
    public function hasTransactions()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction()
    {
        $this->autocommit = false;
    }

    /**
     * Reset autocommit to configured value
     */
    private function resetAutocommit()
    {
        $options = $this->getOptions();
        $this->autocommit = array_key_exists('autocommit', $options) ? $options['autocommit'] : true;
    }

    /**
     * {@inheritdoc}
     */
    public function commitTransaction()
    {
        oci_commit($this->getConnection());
        $this->resetAutocommit();
    }

    /**
     * {@inheritdoc}
     */
    public function rollbackTransaction()
    {
        oci_rollback($this->getConnection());
        $this->resetAutocommit();
    }

    /**
     * {@inheritdoc}
     */
    public function quoteTableName($tableName)
    {
        return $this->quoteSchemaName($this->getSchemaName()) . '.' . $this->quoteColumnName($tableName);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteColumnName($columnName)
    {
        return '"'. $columnName . '"';
    }

    /**
     * {@inheritdoc}
     */
    public function hasTable($tableName)
    {
        $options = $this->getOptions();

        $exists = $this->fetchRow(sprintf(
            "SELECT table_name
             FROM all_tables
             WHERE owner =  '%s' AND table_name = '%s'",
            $options['name'], $tableName
        ));

        return !empty($exists);
    }

    /**
     * {@inheritdoc}
     */
    public function fetch($result)
    {
        return oci_fetch_array($result, OCI_ASSOC | OCI_RETURN_NULLS | OCI_RETURN_LOBS);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchRow($sql)
    {
        $result = $this->query($sql);
        return $this->fetch($result);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($sql)
    {
        $rows = array();
        $result = $this->query($sql);
        while ($row = $this->fetch($result)) {
            $rows[] = $row;
        }
        return $rows;
    }

    /**
     * {@inheritdoc}
     */
    public function createTable(Table $table)
    {
        $this->notImplementedYet();
    }

    /**
     * Gets the SqlServer Column Comment Defininition for a column object.
     *
     * @param Column $column    Column
     * @param string $tableName Table name
     *
     * @return string
     */
    protected function getColumnCommentSqlDefinition(Column $column, $tableName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function renameTable($tableName, $newTableName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function dropTable($tableName)
    {
        $this->notImplementedYet();

    }

    public function getColumnComment($tableName, $columnName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function getColumns($tableName)
    {
        $this->notImplementedYet();

    }

    protected function parseDefault($default)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function hasColumn($tableName, $columnName, $options = array())
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function addColumn(Table $table, Column $column)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function renameColumn($tableName, $columnName, $newColumnName)
    {
        $this->notImplementedYet();

    }

    protected function renameDefault($tableName, $columnName, $newColumnName)
    {
        $this->notImplementedYet();

    }

    public function changeDefault($tableName, Column $newColumn)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function changeColumn($tableName, $columnName, Column $newColumn)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function dropColumn($tableName, $columnName)
    {
        $this->notImplementedYet();

    }

    protected function dropDefaultConstraint($tableName, $columnName)
    {
        $this->notImplementedYet();

    }

    protected function getDefaultConstraint($tableName, $columnName)
    {
        $this->notImplementedYet();

    }

    /**
     * Get an array of indexes from a particular table.
     *
     * @param string $tableName Table Name
     * @return array
     */
    public function getIndexes($tableName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function hasIndex($tableName, $columns)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function hasIndexByName($tableName, $indexName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function addIndex(Table $table, Index $index)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function dropIndex($tableName, $columns)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function dropIndexByName($tableName, $indexName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function hasForeignKey($tableName, $columns, $constraint = null)
    {
        $this->notImplementedYet();

    }

    /**
     * Get an array of foreign keys from a particular table.
     *
     * @param string $tableName Table Name
     * @return array
     */
    protected function getForeignKeys($tableName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function addForeignKey(Table $table, ForeignKey $foreignKey)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function dropForeignKey($tableName, $columns, $constraint = null)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function getSqlType($type, $limit = null)
    {
        $this->notImplementedYet();

    }

    /**
     * Returns Phinx type by SQL type
     *
     * @param $sqlTypeDef
     * @throws \RuntimeException
     * @internal param string $sqlType SQL type
     * @returns string Phinx type
     */
    public function getPhinxType($sqlType)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function createDatabase($name, $options = array())
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function hasDatabase($name)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function dropDatabase($name)
    {
        $this->notImplementedYet();

    }

    /**
     * Get the defintion for a `DEFAULT` statement.
     *
     * @param  mixed $default
     * @return string
     */
    protected function getDefaultValueDefinition($default)
    {
        $this->notImplementedYet();

    }

    /**
     * Gets the SqlServer Column Definition for a Column object.
     *
     * @param Column $column Column
     * @return string
     */
    protected function getColumnSqlDefinition(Column $column, $create = true)
    {
        $this->notImplementedYet();

    }

    /**
     * Gets the SqlServer Index Definition for an Index object.
     *
     * @param Index $index Index
     * @return string
     */
    protected function getIndexSqlDefinition(Index $index, $tableName)
    {
        $this->notImplementedYet();
    }

    /**
     * Gets the SqlServer Foreign Key Definition for an ForeignKey object.
     *
     * @param ForeignKey $foreignKey
     * @return string
     */
    protected function getForeignKeySqlDefinition(ForeignKey $foreignKey, $tableName)
    {
        $this->notImplementedYet();

    }

    /**
     * {@inheritdoc}
     */
    public function getColumnTypes()
    {
        $this->notImplementedYet();
    }

    /**
     * {@inheritdoc}
     */
    public function migrated(MigrationInterface $migration, $direction, $startTime, $endTime) {
        if (strcasecmp($direction, MigrationInterface::UP) === 0) {
            // up
            $sql = sprintf(
                "INSERT INTO %s (version, migration_name, start_time, end_time) VALUES ('%s', '%s', to_date('%s', 'yyyy-mm-dd hh24:mi:ss'), to_date('%s', 'yyyy-mm-dd hh24:mi:ss'))",
                $this->getSchemaTableName(),
                $migration->getVersion(),
                substr($migration->getName(), 0, 100),
                $startTime,
                $endTime
            );

            $this->query($sql);
        } else {
            // down
            $sql = sprintf(
                "DELETE FROM %s WHERE version = '%s'",
                $this->getSchemaTableName(),
                $migration->getVersion()
            );

            $this->query($sql);
        }

        return $this;
    }

    /**
     * {@inheritdoc}
     */
    public function getVersionLog()
    {
        $result = array();
        $rows = $this->fetchAll(sprintf('SELECT * FROM %s ORDER BY version ASC', $this->getSchemaTableName()));
        foreach ($rows as $version) {
            foreach ($version as $key => $value) {
                $version[strtolower($key)] = $value;
            }

            $result[$version['version']] = $version;
        }

        return $result;
    }
}
