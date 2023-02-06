# Проект 2

### Требования:
- Таблицы модели не должны иметь избыточность.
- Из новой модели можно получить те же результаты запрашиваемых данных, что и из старой модели.
- Каждая сущность имеет свои атрибуты в рамках одной таблицы.
- Новая схема должна поддерживать возможность добавления новых акций и промопредложений.
- Из новой модели можно получить те же результаты запрашиваемых данных, что и из старой модели с временем отличающимся не более чем на 1 порядок (x10)
- Доступность (99,99) и надежность должны быть не хуже, чем при старой схеме.

### Особенности данных:
- Порядка 2% заказов исполняется с просрочкой, и это норма для вендоров. Вендор №3, у которого этот процент достигает 10%, скорее всего неблагонадёжен.
- Около 2% заказов не доходят до клиентов.
- В пределах нормы и то, что порядка 1.5% заказов возвращаются клиентами. При этом у вендора №21 50% возвратов. Он торгует электроникой и ноутбуками — очевидно, в его товарах много брака и стоит его изучить.

### Скрипты в папке src:
- DDL.sql - скрипты создания справочников и таблиц
- INSERT.sql - скрипты заполнения данными справочников и таблиц
- SHIPPING_DATAMART.sql - скрипт представления SHIPPING_DATAMART