<?php

declare(strict_types=1);

// Autoloader for use without Composer: require 'path/to/sdks/php/src/autoload.php';
spl_autoload_register(static function (string $class): void {
    if (str_starts_with($class, 'Storine\\')) {
        $file = __DIR__ . '/' . str_replace('\\', '/', substr($class, 8)) . '.php';
        if (is_file($file)) {
            require $file;
        }
    }
});
