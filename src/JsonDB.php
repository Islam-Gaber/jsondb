<?php

namespace JsonSQL;

class JsonSQL
{
    private $dirPath;
    private $data;
    private $query;
    private $conditions;
    private $table;
    private $indexes = [];

    public function __construct($dirPath)
    {
        $this->dirPath = rtrim($dirPath, '/') . '/';
        if (!file_exists($this->dirPath)) {
            mkdir($this->dirPath, 0777, true);
        }
        $this->query = [];
        $this->conditions = [];
    }

    private function getFilePath($tableName)
    {
        return $this->dirPath . $tableName . '.json';
    }

    private function readData($tableName)
    {
        $filePath = $this->getFilePath($tableName);
        if (!file_exists($filePath)) {
            return [];
        }
        $json = file_get_contents($filePath);
        return json_decode($json, true);
    }

    private function writeData($tableName, $data)
    {
        $json = json_encode($data, JSON_PRETTY_PRINT);
        file_put_contents($this->getFilePath($tableName), $json);
    }

    public function createTable($tableName)
    {
        $filePath = $this->getFilePath($tableName);
        if (!file_exists($filePath)) {
            file_put_contents($filePath, json_encode([]));
        }
    }

    public function table($tableName)
    {
        $this->table = $tableName;
        $this->data = $this->readData($tableName);
        return $this;
    }

    public function select($fields = '*')
    {
        $this->query['type'] = 'select';
        $this->query['fields'] = $fields;
        return $this;
    }

    public function insert($record)
    {
        $this->data[] = $record;
        $this->writeData($this->table, $this->data);
        return $this;
    }

    public function update($newValues)
    {
        foreach ($this->data as &$record) {
            if ($this->matchesConditions($record)) {
                $record = array_merge($record, $newValues);
            }
        }
        $this->writeData($this->table, $this->data);
        return $this;
    }

    public function delete()
    {
        $this->data = array_filter($this->data, function ($record) {
            return !$this->matchesConditions($record);
        });
        $this->writeData($this->table, array_values($this->data));
        return $this;
    }

    public function where($field, $operator, $value)
    {
        $this->conditions[] = ['field' => $field, 'operator' => $operator, 'value' => $value, 'type' => 'AND'];
        return $this;
    }

    public function orWhere($field, $operator, $value)
    {
        $this->conditions[] = ['field' => $field, 'operator' => $operator, 'value' => $value, 'type' => 'OR'];
        return $this;
    }

    public function having($field, $operator, $value)
    {
        $this->query['having'][] = compact('field', 'operator', 'value');
        return $this;
    }

    public function distinct($fields = '*')
    {
        $this->query['distinct'] = $fields;
        return $this;
    }

    public function count()
    {
        return count($this->get());
    }

    public function sum($field)
    {
        return array_sum(array_column($this->get(), $field));
    }

    public function avg($field)
    {
        $values = array_column($this->get(), $field);
        return array_sum($values) / count($values);
    }

    public function min($field)
    {
        return min(array_column($this->get(), $field));
    }

    public function max($field)
    {
        return max(array_column($this->get(), $field));
    }

    public function pluck($field)
    {
        return array_column($this->get(), $field);
    }

    public function like($field, $pattern)
    {
        $this->conditions[] = ['field' => $field, 'operator' => 'like', 'value' => $pattern, 'type' => 'AND'];
        return $this;
    }

    public function join($otherTable, $localKey, $foreignKey, $type = 'inner')
    {
        $this->query['join'] = compact('otherTable', 'localKey', 'foreignKey', 'type');
        return $this;
    }

    public function groupBy($field)
    {
        $this->query['groupBy'] = $field;
        return $this;
    }

    public function orderBy($field, $direction = 'asc')
    {
        $this->query['orderBy'] = compact('field', 'direction');
        return $this;
    }

    public function limit($limit)
    {
        $this->query['limit'] = $limit;
        return $this;
    }

    public function offset($offset)
    {
        $this->query['offset'] = $offset;
        return $this;
    }

    public function merge($newRecords, $key)
    {
        $existingRecords = $this->data;
        foreach ($newRecords as $newRecord) {
            $found = false;
            foreach ($existingRecords as &$existingRecord) {
                if ($existingRecord[$key] == $newRecord[$key]) {
                    $existingRecord = array_merge($existingRecord, $newRecord);
                    $found = true;
                    break;
                }
            }
            if (!$found) {
                $existingRecords[] = $newRecord;
            }
        }
        $this->writeData($this->table, $existingRecords);
        return $this;
    }

    public function truncate()
    {
        $this->writeData($this->table, []);
        return $this;
    }

    public function random($count)
    {
        $results = $this->get();
        shuffle($results);
        return array_slice($results, 0, $count);
    }

    public function between($field, $start, $end)
    {
        $this->conditions[] = ['field' => $field, 'operator' => 'between', 'value' => [$start, $end], 'type' => 'AND'];
        return $this;
    }

    public function notBetween($field, $start, $end)
    {
        $this->conditions[] = ['field' => $field, 'operator' => 'notBetween', 'value' => [$start, $end], 'type' => 'AND'];
        return $this;
    }

    public function exists()
    {
        return !empty($this->get());
    }

    public function first()
    {
        return reset($this->get());
    }

    public function last()
    {
        $results = $this->get();
        return end($results);
    }

    public function pluckWhere($field, $conditionField, $conditionValue)
    {
        return array_column(array_filter($this->get(), function ($record) use ($conditionField, $conditionValue) {
            return $record[$conditionField] == $conditionValue;
        }), $field);
    }

    public function find($field, $value)
    {
        return array_values(array_filter($this->get(), function ($record) use ($field, $value) {
            return $record[$field] == $value;
        }))[0] ?? null;
    }

    public function findMany($field, $values)
    {
        return array_values(array_filter($this->get(), function ($record) use ($field, $values) {
            return in_array($record[$field], $values);
        }));
    }

    public function updateOrInsert($record, $key)
    {
        $existingRecords = $this->data;
        $found = false;
        foreach ($existingRecords as &$existingRecord) {
            if ($existingRecord[$key] == $record[$key]) {
                $existingRecord = array_merge($existingRecord, $record);
                $found = true;
                break;
            }
        }
        if (!$found) {
            $existingRecords[] = $record;
        }
        $this->writeData($this->table, $existingRecords);
        return $this;
    }

    public function selectRaw($callback)
    {
        return $callback($this->data);
    }

    public function toArray()
    {
        return json_decode($this->readData($this->table), true);
    }

    public function jsonExport($fileName)
    {
        file_put_contents($fileName, json_encode($this->data, JSON_PRETTY_PRINT));
        return $this;
    }

    public function jsonImport($fileName)
    {
        $this->data = json_decode(file_get_contents($fileName), true);
        return $this;
    }

    public function pluckDistinct($field)
    {
        return array_values(array_unique(array_column($this->get(), $field)));
    }

    public function orderByRaw($callback)
    {
        $this->data = $callback($this->data);
        return $this;
    }

    public function removeDuplicates($field)
    {
        $unique = [];
        $filtered = [];
        foreach ($this->data as $record) {
            if (!in_array($record[$field], $unique)) {
                $unique[] = $record[$field];
                $filtered[] = $record;
            }
        }
        $this->data = $filtered;
        return $this;
    }

    public function cleanEmpty($field)
    {
        $this->data = array_filter($this->data, function ($record) use ($field) {
            return !empty($record[$field]);
        });
        return $this;
    }

    public function replaceNulls($field, $defaultValue)
    {
        foreach ($this->data as &$record) {
            if (is_null($record[$field])) {
                $record[$field] = $defaultValue;
            }
        }
        return $this;
    }

    public function normalize($field, $callback)
    {
        foreach ($this->data as &$record) {
            if (isset($record[$field])) {
                $record[$field] = $callback($record[$field]);
            }
        }
        return $this;
    }

    public function extractColumn($field)
    {
        $values = array_column($this->data, $field);
        return [
            'mean' => array_sum($values) / count($values),
            'min' => min($values),
            'max' => max($values),
            'std_dev' => sqrt(array_sum(array_map(function ($v) use ($values) {
                return pow($v - (array_sum($values) / count($values)), 2);
            }, $values)) / count($values))
        ];
    }

    public function filterByRange($field, $start, $end)
    {
        $this->data = array_filter($this->data, function ($record) use ($field, $start, $end) {
            return isset($record[$field]) && $record[$field] >= $start && $record[$field] <= $end;
        });
        return $this;
    }

    public function convertToArray()
    {
        return array_map(function ($record) {
            return (array) $record;
        }, $this->data);
    }

    public function createIndex($field)
    {
        $index = [];
        foreach ($this->data as $record) {
            if (isset($record[$field])) {
                $index[$record[$field]][] = $record;
            }
        }
        $this->indexes[$field] = $index;
        return $this;
    }

    public function queryIndex($field, $value)
    {
        return $this->indexes[$field][$value] ?? [];
    }

    public function whereNested($conditions)
    {
        $this->conditions[] = ['type' => 'nested', 'conditions' => $conditions];
        return $this;
    }

    private function applyNestedConditions($records, $conditions)
    {
        // Apply each condition in the nested array
        foreach ($conditions as $condition) {
            if ($condition['type'] == 'nested') {
                // Recursively apply nested conditions
                $records = $this->applyNestedConditions($records, $condition['conditions']);
            } else {
                // Apply a single condition
                $records = array_filter($records, function ($record) use ($condition) {
                    return $this->evaluateCondition($record, $condition);
                });
            }
        }
        return $records;
    }


    private function evaluateCondition($record, $condition)
    {
        $field = $condition['field'];
        $operator = $condition['operator'];
        $value = $condition['value'];

        if (!isset($record[$field])) {
            return false;
        }

        return $this->compare($record[$field], $operator, $value);
    }


    public function backup($backupFile)
    {
        file_put_contents($backupFile, json_encode($this->data, JSON_PRETTY_PRINT));
        return $this;
    }

    public function restore($backupFile)
    {
        $this->data = json_decode(file_get_contents($backupFile), true);
        return $this;
    }

    public function encryptData($key)
    {
        $this->data = array_map(function ($record) use ($key) {
            return openssl_encrypt(json_encode($record), 'AES-128-ECB', $key);
        }, $this->data);
        return $this;
    }

    public function decryptData($key)
    {
        $this->data = array_map(function ($record) use ($key) {
            return json_decode(openssl_decrypt($record, 'AES-128-ECB', $key), true);
        }, $this->data);
        return $this;
    }


    public function get()
    {
        $results = array_filter($this->data, function ($record) {
            return $this->matchesConditions($record);
        });

        if (isset($this->query['join'])) {
            $joinTableData = $this->readData($this->query['join']['otherTable']);
            $results = $this->performJoin($results, $joinTableData, $this->query['join']);
        }

        if (isset($this->query['groupBy'])) {
            $results = $this->performGroupBy($results, $this->query['groupBy']);
        }

        if (isset($this->query['orderBy'])) {
            $results = $this->performOrderBy($results, $this->query['orderBy']);
        }

        if (isset($this->query['offset'])) {
            $results = array_slice($results, $this->query['offset']);
        }

        if (isset($this->query['limit'])) {
            $results = array_slice($results, 0, $this->query['limit']);
        }

        if (isset($this->query['distinct'])) {
            $results = $this->performDistinct($results, $this->query['distinct']);
        }

        if (isset($this->query['having'])) {
            $results = array_filter($results, function ($record) {
                foreach ($this->query['having'] as $condition) {
                    if (!$this->compare($record[$condition['field']] ?? null, $condition['operator'], $condition['value'])) {
                        return false;
                    }
                }
                return true;
            });
        }

        if ($this->query['fields'] == '*') {
            return $results;
        }

        return array_map(function ($record) {
            return array_intersect_key($record, array_flip((array) $this->query['fields']));
        }, $results);
    }

    private function performJoin($baseData, $joinData, $joinQuery)
    {
        $result = [];
        foreach ($baseData as $baseRecord) {
            foreach ($joinData as $joinRecord) {
                if ($baseRecord[$joinQuery['localKey']] == $joinRecord[$joinQuery['foreignKey']]) {
                    $result[] = array_merge($baseRecord, $joinRecord);
                }
            }
        }
        return $result;
    }

    private function performGroupBy($data, $field)
    {
        $grouped = [];
        foreach ($data as $record) {
            $key = $record[$field];
            if (!isset($grouped[$key])) {
                $grouped[$key] = [];
            }
            $grouped[$key][] = $record;
        }
        return $grouped;
    }

    private function performOrderBy($data, $orderQuery)
    {
        usort($data, function ($a, $b) use ($orderQuery) {
            if ($a[$orderQuery['field']] == $b[$orderQuery['field']]) {
                return 0;
            }
            return ($a[$orderQuery['field']] < $b[$orderQuery['field']]) ? ($orderQuery['direction'] == 'asc' ? -1 : 1) : ($orderQuery['direction'] == 'asc' ? 1 : -1);
        });
        return $data;
    }

    private function performDistinct($data, $fields)
    {
        $seen = [];
        $result = [];
        foreach ($data as $item) {
            $key = '';
            foreach ((array) $fields as $field) {
                $key .= $item[$field] . '|';
            }
            if (!isset($seen[$key])) {
                $seen[$key] = true;
                $result[] = $item;
            }
        }
        return $result;
    }

    private function matchesConditions($record)
    {
        $conditions = $this->conditions;
        $result = true;

        foreach ($conditions as $condition) {
            if ($condition['type'] == 'AND') {
                $result = $result && $this->compare($record[$condition['field']] ?? null, $condition['operator'], $condition['value']);
            } elseif ($condition['type'] == 'OR') {
                $result = $result || $this->compare($record[$condition['field']] ?? null, $condition['operator'], $condition['value']);
            }
        }

        return $result;
    }

    private function compare($a, $operator, $b)
    {
        switch ($operator) {
            case '=':
                return $a == $b;
            case '!=':
                return $a != $b;
            case '<':
                return $a < $b;
            case '>':
                return $a > $b;
            case '<=':
                return $a <= $b;
            case '>=':
                return $a >= $b;
            case 'like':
                return fnmatch($b, $a);
            default:
                return false;
        }
    }
}
