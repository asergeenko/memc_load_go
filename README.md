# memcached log loader
Многопоточная реализация загрузчика логов в memcached

### Пример строки лог-файла
idfa&nbsp;&nbsp;&nbsp;&nbsp;1rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.5542.42&nbsp;&nbsp;&nbsp;&nbsp;1423,43,567,3,7,23

idfa&nbsp;&nbsp;&nbsp;&nbsp;2rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;2423,43,567,3,7,23

idfa&nbsp;&nbsp;&nbsp;&nbsp;3rfw452y52g2gq4g&nbsp;&nbsp;&nbsp;&nbsp;55.55&nbsp;&nbsp;&nbsp;&nbsp;42.42&nbsp;&nbsp;&nbsp;&nbsp;3423,43,567,3,7,23

## Запуск
```memc_load [-w WORKERS] [-l LOG] [--idfa] [--gaid] [--adid] [--dvid]```

### Параметры

- *WORKERS* - количество worker'ов (по умолчанию 5)

- *LOG* - лог-файл

- *--idfa*, *--gaid*, *--adid*, *--dvid* - строка подключения к memcached (хост:порт) для соответствующего типа устройства

## Пакеты
[gomemcache](https://github.com/bradfitz/gomemcache)

[protobuf](https://pkg.go.dev/google.golang.org/protobuf)