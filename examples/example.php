<?php

require_once 'JsonSQL.php';

use JsonSQL\JsonSQL;

// تحديد مسار الدليل حيث سيتم تخزين قواعد البيانات JSON
$dirPath = 'data';

// إنشاء مثيل من فئة JsonSQL
$db = new JsonSQL($dirPath);

// إنشاء جدول جديد باسم 'users'
$db->createTable('users');

// إدخال بعض البيانات في جدول 'users'
$db->table('users')->insert([
    ['id' => 1, 'name' => 'John Doe', 'email' => 'john@example.com', 'age' => 30],
    ['id' => 2, 'name' => 'Jane Smith', 'email' => 'jane@example.com', 'age' => 25],
    ['id' => 3, 'name' => 'Alice Johnson', 'email' => 'alice@example.com', 'age' => 28],
]);

// استعلام البيانات من جدول 'users'
$users = $db->table('users')->select()->get();
print_r($users);

// تحديث بيانات المستخدم الذي يحمل معرف 1
$db->table('users')->where('id', '=', 1)->update(['age' => 31]);

// حذف المستخدم الذي يحمل معرف 2
$db->table('users')->where('id', '=', 2)->delete();

// إضافة بيانات جديدة إلى جدول 'users'
$db->table('users')->insert([
    ['id' => 4, 'name' => 'Bob Brown', 'email' => 'bob@example.com', 'age' => 22],
]);

// عرض جميع المستخدمين بعد التحديث
$users = $db->table('users')->select()->get();
print_r($users);

// استخدام الاستعلامات المعقدة
$db->table('users')
    ->where('age', '>', 20)
    ->orWhere('name', 'like', 'Alice%')
    ->orderBy('age', 'desc')
    ->limit(2);

$filteredUsers = $db->get();
print_r($filteredUsers);

// إنشاء فهرس على الحقل 'email'
$db->table('users')->createIndex('email');

// استعلام باستخدام الفهرس
$usersWithEmail = $db->table('users')->queryIndex('email', 'alice@example.com');
print_r($usersWithEmail);

// عمل نسخة احتياطية من البيانات
$db->table('users')->backup('backup.json');

// استعادة البيانات من النسخة الاحتياطية
$db->table('users')->restore('backup.json');

// تشفير البيانات
$db->table('users')->encryptData('secret-key');

// فك تشفير البيانات
$db->table('users')->decryptData('secret-key');

// حذف الجدول 'users'
$db->table('users')->truncate();

?>
