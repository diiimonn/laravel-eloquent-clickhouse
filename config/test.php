<?php

return [
    'single' => [
        'host' => '127.0.0.1',
        'port' => 8123,
        'database' => 'app',
        'username' => 'app',
        'password' => 'app',
        'options' => [
            'timeout' => 15,
            'protocol' => 'http',
        ],
    ],

    'servers' => [
        [
            'host' => 'localhost',
            'port' => 8123,
            'database' => 'default',
            'username' => 'default',
            'password' => '',
            'options' => [
                'timeout' => 10,
                'protocol' => 'http',
            ],
        ],
    ],

    'clusters' => [
        'test' => [
            'server-1' => [
                'host' => 'localhost',
                'port' => 8123,
                'database' => 'default',
                'username' => 'default',
                'password' => '',
            ],
            'server2' => [
                'host' => 'localhost',
                'port' => 8123,
                'database' => 'default',
                'username' => 'default',
                'password' => '',
                'options' => [
                    'timeout ' => 10,
                ],
            ],
            'server3' => [
                'host' => 'not_local_host',
                'port' => 8123,
                'database' => 'default',
                'username' => 'default',
                'password' => '',
                'options' => [
                    'timeout' => 10,
                ],
            ],
        ],
    ],

    'servers-tags' => [
        [
            'host' => 'with-tag',
            'port' => 8123,
            'database' => 'default',
            'username' => 'default',
            'password' => '',
            'options' => [
                'tags' => [
                    'tag',
                ],
            ],
        ],
        [
            'host' => 'without-tag',
            'port' => 8123,
            'database' => 'default',
            'username' => 'default',
            'password' => '',
        ],
    ],

    'clusters-tags' => [
        'test' => [
            [
                'host' => 'with-tag',
                'port' => 8123,
                'database' => 'default',
                'username' => 'default',
                'password' => '',
                'options' => [
                    'tags' => [
                        'tag',
                    ],
                ],
            ],
            [
                'host' => 'without-tag',
                'port' => 8123,
                'database' => 'default',
                'username' => 'default',
                'password' => '',
            ],
        ],
    ],

];
